use bobs::benchmark::config::{BenchmarkConfig, ParseOutcome};
use bobs::benchmark::run::run_benchmark;
use bobs::benchmark::summary::{render_human, render_summary_line};
use std::fs::File;
use std::io::Write;

#[tokio::main]
async fn main() {
    let cfg = match BenchmarkConfig::parse_args_from(std::env::args()) {
        Ok(ParseOutcome::Help(text)) => {
            print!("{text}");
            std::process::exit(0);
        }
        Ok(ParseOutcome::Run(cfg)) => cfg,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", bobs::benchmark::config::usage());
            std::process::exit(2);
        }
    };

    let summary_path = cfg.summary_json.clone();
    let results_path = cfg.results_jsonl.clone();
    match run_benchmark(cfg).await {
        Ok(summary) => {
            if let Some(path) = summary_path {
                if let Err(e) = std::fs::write(
                    &path,
                    serde_json::to_vec_pretty(&summary).expect("summary json"),
                ) {
                    eprintln!("error writing --summary-json {path}: {e}");
                }
            }
            if let Some(path) = results_path {
                if let Err(e) = write_results_jsonl(&path, &summary.results) {
                    eprintln!("error writing --results-jsonl {path}: {e}");
                }
            }
            println!("{}", render_human(&summary));
            println!("{}", render_summary_line(&summary));
            std::process::exit(if summary.failures == 0 { 0 } else { 1 });
        }
        Err(e) => {
            eprintln!("benchmark setup failed: {e}");
            std::process::exit(1);
        }
    }
}

fn write_results_jsonl(
    path: &str,
    results: &[bobs::benchmark::summary::ObjectResult],
) -> std::io::Result<()> {
    let mut f = File::create(path)?;
    for r in results {
        writeln!(f, "{}", serde_json::to_string(r).expect("result json"))?;
    }
    Ok(())
}
