pub mod binaries;

pub use binaries::{
    check_extensions, discover, find_major, newest, parse_version, read_pg_version, PgInstall,
};
