//! End-to-end: mock `/metrics` → scrape → registry mirror → cold Parquet.
//!
//! Uses short retention/compaction so the flow completes in seconds. The
//! registry text proves mapping + recording; the cold file proves the
//! Fjall → Parquet leg (tonggeret's engine owns the middle).

use std::time::Duration;

use tonggeret_dashboard::config::TargetConfig;

const EXPOSITION: &str = "# TYPE http_requests_total counter\n\
     http_requests_total{method=\"GET\",path=\"/\",status=\"200\"} 7\n\
     # TYPE http_request_duration_ms histogram\n\
     http_request_duration_ms_bucket{le=\"5\"} 3\n\
     http_request_duration_ms_bucket{le=\"+Inf\"} 7\n\
     http_request_duration_ms_sum 42\n\
     http_request_duration_ms_count 7\n\
     # TYPE visitors_total counter\n\
     visitors_total{region=\"DE\"} 5\n\
     # TYPE unique_visitors_estimate gauge\n\
     unique_visitors_estimate{region=\"DE\"} 4\n\
     # TYPE something_else gauge\n\
     something_else 1\n";

async fn mock_metrics() -> &'static str {
    EXPOSITION
}

#[tokio::test]
async fn scrape_flows_to_registry_and_cold_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let cold = tmp.path().join("cold");
    std::fs::create_dir_all(&cold).unwrap();

    let mut fcfg = tonggeret::FjallConfig::new(tmp.path().join("fjall"));
    fcfg.cold_storage_dir = Some(cold.clone());
    fcfg.retention = Duration::from_secs(2);
    fcfg.compaction_interval = Duration::from_secs(1);
    tonggeret::init(tonggeret::Config::default_light().with_fjall(fcfg)).unwrap();

    let app = axum::Router::new().route("/metrics", axum::routing::get(mock_metrics));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Mock target: axum 0.8 (dev-dependency) serving static exposition.
    let target = TargetConfig {
        name: "mock".to_string(),
        url: format!("http://{addr}/metrics"),
        allow: ["http_", "visitors_", "unique_", "tonggeret_", "beruang_"]
            .iter()
            .map(ToString::to_string)
            .collect(),
    };
    let client = reqwest::Client::new();
    let status = tonggeret_dashboard::scrape::scrape_once(&client, &target, 5_000, 1_048_576).await;
    assert_eq!(status, tonggeret_dashboard::scrape::ScrapeStatus::Ok);

    // Registry mirror proves mapping + recording (incl. histogram buckets
    // and the optional visitor series); `something_else` must be absent.
    let text = tonggeret::prometheus_text().unwrap();
    assert!(text.contains("http_requests_total"));
    assert!(text.contains("http_request_duration_ms_bucket"));
    assert!(text.contains("visitors_total"));
    assert!(text.contains("unique_visitors_estimate"));
    assert!(!text.contains("something_else"));

    // Compaction exports keys older than retention (2s) on a 1s interval.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let files: Vec<_> = std::fs::read_dir(&cold)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        !files.is_empty(),
        "expected at least one cold parquet export"
    );
    assert!(
        files
            .iter()
            .all(|e| e.path().extension().is_some_and(|x| x == "parquet")),
        "only parquet files expected"
    );
    assert!(
        files
            .iter()
            .any(|e| e.metadata().is_ok_and(|m| m.len() > 0)),
        "expected a non-empty export"
    );
    tonggeret::shutdown().unwrap();
}
