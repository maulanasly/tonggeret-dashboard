//! `tonggeret-dashboard` collector library: config, scrape pipeline, serving.
//!
//! The binary (`collector`) wires these together: scrape targets on an
//! interval, store through the tonggeret engine, serve the UI + history.

pub mod config;
pub mod query;
pub mod scrape;
pub mod serve;
pub mod status;
