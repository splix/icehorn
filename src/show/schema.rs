//! `show schema` — read the latest metadata.json and print the columns of
//! the current schema as a table.

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;

use crate::cli::TableArgs;
use crate::config::S3Config;
use crate::iceberg::metadata::{find_latest, read_json};
use crate::iceberg::model::{Metadata, SchemaField};
use crate::s3_url::S3Location;

pub async fn run(args: TableArgs) -> Result<()> {
    let config = S3Config::from_file(&args.config)?;
    let location = S3Location::parse(&args.location)?;
    let store = config.build_store(&location.bucket)?;

    let metadata_prefix = ObjPath::from(format!("{}/metadata", location.prefix));
    let latest = find_latest(&store, &metadata_prefix)
        .await?
        .context("no <NNNNN>-<uuid>.metadata.json files found under metadata/")?;

    let bytes = read_json(&store, &latest.path).await?;
    let meta: Metadata =
        serde_json::from_slice(&bytes).context("failed to parse metadata JSON")?;

    let Some(schema) = meta.current_schema() else {
        println!("No current schema found in metadata.");
        return Ok(());
    };

    println!("Schema id: {}", schema.schema_id);
    println!();
    print_fields(&schema.fields);

    Ok(())
}

/// Render each column's `type` for display. Primitive types come through as
/// JSON strings (e.g. `"long"`, `"decimal(9, 2)"`) and we print them as-is;
/// nested struct/list/map types arrive as objects which we collapse into a
/// short, parser-style summary so a single row stays readable.
fn render_type(t: &serde_json::Value) -> String {
    if let Some(s) = t.as_str() {
        return s.to_string();
    }
    let Some(obj) = t.as_object() else {
        return t.to_string();
    };
    match obj.get("type").and_then(|v| v.as_str()) {
        Some("struct") => {
            let n = obj
                .get("fields")
                .and_then(|f| f.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            format!("struct<{n} fields>")
        }
        Some("list") => {
            let element = obj
                .get("element")
                .map(render_type)
                .unwrap_or_else(|| "?".to_string());
            format!("list<{element}>")
        }
        Some("map") => {
            let key = obj
                .get("key")
                .map(render_type)
                .unwrap_or_else(|| "?".to_string());
            let value = obj
                .get("value")
                .map(render_type)
                .unwrap_or_else(|| "?".to_string());
            format!("map<{key}, {value}>")
        }
        Some(other) => other.to_string(),
        None => t.to_string(),
    }
}

fn print_fields(fields: &[SchemaField]) {
    if fields.is_empty() {
        println!("(schema has no columns)");
        return;
    }

    let headers = ["id", "name", "type", "required"];
    let rows: Vec<[String; 4]> = fields
        .iter()
        .map(|f| {
            [
                f.id.to_string(),
                f.name.clone(),
                render_type(&f.r#type),
                if f.required { "yes" } else { "no" }.to_string(),
            ]
        })
        .collect();

    let mut widths = headers.map(str::len);
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    print_row(&headers.map(String::from), &widths);
    print_separator(&widths);
    for row in &rows {
        print_row(row, &widths);
    }
}

fn print_row(cells: &[String; 4], widths: &[usize; 4]) {
    println!(
        "{:<w0$} | {:<w1$} | {:<w2$} | {:<w3$}",
        cells[0],
        cells[1],
        cells[2],
        cells[3],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
    );
}

fn print_separator(widths: &[usize; 4]) {
    println!(
        "{:-<w0$}-+-{:-<w1$}-+-{:-<w2$}-+-{:-<w3$}",
        "",
        "",
        "",
        "",
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn render_type_handles_primitive_strings() {
        assert_eq!(render_type(&json!("long")), "long");
        assert_eq!(render_type(&json!("decimal(9, 2)")), "decimal(9, 2)");
    }

    #[test]
    fn render_type_collapses_struct_list_map() {
        assert_eq!(
            render_type(&json!({
                "type": "struct",
                "fields": [{"id": 1}, {"id": 2}, {"id": 3}]
            })),
            "struct<3 fields>"
        );
        assert_eq!(
            render_type(&json!({"type": "list", "element": "string"})),
            "list<string>"
        );
        assert_eq!(
            render_type(&json!({"type": "map", "key": "string", "value": "long"})),
            "map<string, long>"
        );
    }

    #[test]
    fn render_type_falls_back_for_unknown_shapes() {
        // A nested map<string, struct<...>> exercises the recursive path
        // and the struct field-count summary at the same time.
        assert_eq!(
            render_type(&json!({
                "type": "map",
                "key": "string",
                "value": {"type": "struct", "fields": [{"id": 1}, {"id": 2}]}
            })),
            "map<string, struct<2 fields>>"
        );
    }
}
