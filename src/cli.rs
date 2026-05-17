use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "icehorn", about = "Iceberg data lake utilities")]
pub struct Cli {
    /// Disable the interactive TUI and print plain text output. Useful
    /// when running under CI or when the output is being piped — anywhere
    /// a live terminal can't be assumed.
    #[arg(long, global = true)]
    pub plain: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Copy Iceberg tables from one S3 location to another, rewriting
    /// embedded metadata paths
    Copy(CopyArgs),

    /// Inspect information about an Iceberg table
    Show {
        #[command(subcommand)]
        command: ShowCommand,
    },
}

#[derive(clap::Args)]
pub struct CopyArgs {
    /// Path to s3cmd config file for the source S3
    #[arg(long = "from.config")]
    pub from_config: PathBuf,

    /// Source S3 URL pointing at a namespace that contains tables as
    /// subdirectories (e.g. `s3://archive/iceberg/<namespace-uuid>`)
    #[arg(long)]
    pub from: String,

    /// Path to s3cmd config file for the destination S3
    #[arg(long = "to.config")]
    pub to_config: PathBuf,

    /// Destination S3 URL — same namespace shape as `--from`
    #[arg(long)]
    pub to: String,

    /// If set, copy only this specific table UUID within the
    /// namespace pointed to by `--from`. Required when `--scope
    /// snapshot:<id>` is used (snapshot ids are per-table). Without
    /// this flag, every table under `--from` is copied.
    #[arg(long = "table")]
    pub table: Option<String>,

    /// What to copy:
    ///   `latest` (default) — only files reachable from each table's
    ///                        current snapshot, plus snapshots whose
    ///                        manifest-list is already present at the
    ///                        destination from a previous run
    ///   `all`              — every file under the source prefix
    ///   `<NNNNN>`          — files reachable from a specific metadata
    ///                        version (e.g. `5` or `00005`)
    ///   `snapshot:<ID>`    — files reachable from whichever metadata
    ///                        file carries `<ID>` as its current
    ///                        snapshot — handy when a catalog
    ///                        (Lakekeeper, Polaris, …) hands you a
    ///                        snapshot id but not the metadata version
    #[arg(long, default_value = "latest", value_parser = Scope::parse)]
    pub scope: Scope,

    /// In-flight file copies per table (default: 32)
    #[arg(long = "copy.parallel", default_value_t = 32)]
    pub copy_parallel: usize,

    /// Maximum tables in the file-copy phase at once (default: 4).
    /// Discovery (manifest walk + destination scan) runs in parallel
    /// for every table regardless, so a small table can finish its
    /// scan and start copying while large tables are still scanning.
    /// Files within each table use `--copy.parallel`.
    #[arg(long = "tables.parallel", default_value_t = 4)]
    pub tables_parallel: usize,
}

/// What slice of an Iceberg table to copy. See `--scope` on `CopyArgs`
/// for the description of each variant.
#[derive(Debug, Clone, Copy)]
pub enum Scope {
    Latest,
    All,
    Version(u32),
    /// Resolve the metadata file by its current snapshot id — useful
    /// when the source is managed by a REST catalog that hands you a
    /// snapshot id rather than a metadata file path.
    Snapshot(i64),
}

impl Scope {
    fn parse(s: &str) -> Result<Self, String> {
        if let Some(rest) = s.strip_prefix("snapshot:") {
            return rest
                .parse::<i64>()
                .map(Scope::Snapshot)
                .map_err(|_| format!("expected 'snapshot:<int64-id>'; got '{s}'"));
        }
        match s {
            "latest" => Ok(Scope::Latest),
            "all" => Ok(Scope::All),
            other => other.parse::<u32>().map(Scope::Version).map_err(|_| {
                format!(
                    "expected 'latest', 'all', a metadata version number, or 'snapshot:<id>'; got '{other}'"
                )
            }),
        }
    }
}

#[derive(Subcommand)]
pub enum ShowCommand {
    /// Print the current metadata version and filename UUID
    Version(TableArgs),

    /// Print the current snapshot and a few preceding snapshots
    Snapshot(TableArgs),

    /// Print the columns of the current schema as a table
    Schema(TableArgs),

    /// List the Iceberg tables under a given S3 location. The location
    /// may point at a single table, a namespace, or the root that holds
    /// multiple namespaces — the level is auto-detected.
    Tables(LocationArgs),
}

/// Parameters that identify a single Iceberg table on S3. Shared by every
/// `show <subcommand>` so each one stays consistent on the command line.
#[derive(clap::Args)]
pub struct TableArgs {
    /// Path to s3cmd config file
    #[arg(long)]
    pub config: PathBuf,

    /// S3 URL of the Iceberg table (e.g. s3://bucket/path/to/table)
    #[arg(long)]
    pub location: String,
}

/// Parameters for commands whose `--location` can point at any level of
/// an Iceberg layout (table, namespace, or root). Kept separate from
/// `TableArgs` so the `--location` help text stays accurate for each.
#[derive(clap::Args)]
pub struct LocationArgs {
    /// Path to s3cmd config file
    #[arg(long)]
    pub config: PathBuf,

    /// S3 URL to inspect. May point at a table
    /// (`s3://bucket/<prefix>/<ns>/<table>`), a namespace
    /// (`s3://bucket/<prefix>/<ns>`), or a root holding namespaces
    /// (`s3://bucket/<prefix>`). The level is detected automatically.
    #[arg(long)]
    pub location: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_parses_keywords() {
        assert!(matches!(Scope::parse("latest").unwrap(), Scope::Latest));
        assert!(matches!(Scope::parse("all").unwrap(), Scope::All));
    }

    #[test]
    fn scope_parses_version_number() {
        assert!(matches!(Scope::parse("5").unwrap(), Scope::Version(5)));
        // Leading zeros are tolerated — Iceberg pads metadata versions to 5
        // digits, so users naturally type the padded form.
        assert!(matches!(Scope::parse("00005").unwrap(), Scope::Version(5)));
    }

    #[test]
    fn scope_rejects_garbage() {
        assert!(Scope::parse("nope").is_err());
        assert!(Scope::parse("").is_err());
    }

    #[test]
    fn scope_parses_snapshot_id() {
        assert!(matches!(
            Scope::parse("snapshot:5847308092535477582").unwrap(),
            Scope::Snapshot(5847308092535477582)
        ));
        // Negative ids are accepted (the spec doesn't forbid them); only
        // non-numeric tails are rejected.
        assert!(matches!(
            Scope::parse("snapshot:-12345").unwrap(),
            Scope::Snapshot(-12345)
        ));
        assert!(Scope::parse("snapshot:not-a-number").is_err());
        assert!(Scope::parse("snapshot:").is_err());
    }
}
