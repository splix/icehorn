//! Shared Iceberg primitives — file naming, metadata.json model, and
//! manifest walking. Used by both the `show` and `copy` commands so they
//! agree on what counts as a metadata file and how a snapshot's reachable
//! files are discovered.
//!
//! See `reference/iceberg-naming-conventions.md` and
//! `reference/iceberg-s3-migration.md` for the spec details that drive
//! the parsers and walkers below.

pub mod layout;
pub mod manifest;
pub mod metadata;
pub mod model;
