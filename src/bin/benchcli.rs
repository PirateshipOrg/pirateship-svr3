/*
Accepts a list of server addresses, and threshold as a json file input.
Accepts client name as a command line argument.
Accepts a burst rate as a command line argument.

Client first does a store operation.
Then, it spawns #burst number of tasks, and repeatedly sends restore requests with the correct password
using all tasks in parallel and then wait for all tasks to complete.

Runs this for 10_000 iterations and then prints the cdf of latencies.
*/

use std::time::Instant;
use pirateship_svr3::{
    client::{RecoveryClient, ClientConfig},
    log_config::default_log4rs_config,
};

const ITERATIONS: usize = 1000;
const SECRET: &[u8] = b"benchmark_secret_key_benchmark_secret_key_benchmark_secret_key_b";

/// Prints usage information for the benchmark client.
fn print_usage(program_name: &str) {
    eprintln!("Usage: {} <client_name> <burst_rate> <config_file>", program_name);
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  client_name    Client identifier");
    eprintln!("  burst_rate     Number of parallel restore tasks per iteration");
    eprintln!("  config_file    Path to JSON configuration file");
    eprintln!();
    eprintln!("Config file format:");
    eprintln!("  {{");
    eprintln!("    \"server_urls\": [\"http://127.0.0.1:5001\", ...],");
    eprintln!("    \"server_threshold\": 2,");
    eprintln!("    \"server_count\": 4");
    eprintln!("  }}");
    eprintln!();
    eprintln!("Example:");
    eprintln!("  {} client1 10 configs/client_config.json", program_name);
}

/// Reads JSON configuration from a file.
fn read_config(config_path: &str) -> Result<String, std::io::Error> {
    std::fs::read_to_string(config_path)
}

/// Parses command line arguments.
fn parse_args() -> Result<(String, usize, String), String> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() != 4 {
        return Err("Invalid number of arguments".to_string());
    }
    
    let client_name = args[1].clone();
    let burst_rate = args[2].parse::<usize>()
        .map_err(|e| format!("Invalid burst_rate: {}", e))?;
    let config_file = args[3].clone();
    
    if burst_rate == 0 {
        return Err("burst_rate must be greater than 0".to_string());
    }
    
    Ok((client_name, burst_rate, config_file))
}

/// Computes and prints the CDF (Cumulative Distribution Function) of latencies.
fn print_cdf(latencies: &[f64]) {
    if latencies.is_empty() {
        eprintln!("No latencies to compute CDF");
        return;
    }
    
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let n = sorted.len();
    
    // Print percentiles
    let percentiles = vec![
        0.0, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 97.5, 99.0, 99.5, 100.0
    ];
    
    println!("\nLatency CDF (in milliseconds):");
    println!("{:<10} {:<15} {:<15}", "Percentile", "Latency (ms)", "Count");
    println!("{}", "-".repeat(40));
    
    for percentile in percentiles {
        let index = if percentile == 100.0 {
            n - 1
        } else {
            ((percentile / 100.0) * (n - 1) as f64).round() as usize
        };
        
        let latency_ms = sorted[index];
        let count = index + 1;
        
        println!("{:<10.1}% {:<15.2} {:<15}", percentile, latency_ms, count);
    }
    
    // Print summary statistics
    let sum: f64 = sorted.iter().sum();
    let mean = sum / n as f64;
    let median = sorted[n / 2];
    
    let variance: f64 = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
    let std_dev = variance.sqrt();
    
    println!("\nSummary Statistics:");
    println!("  Total requests: {}", n);
    println!("  Mean: {:.2} ms", mean);
    println!("  Median: {:.2} ms", median);
    println!("  Std Dev: {:.2} ms", std_dev);
    println!("  Min: {:.2} ms", sorted[0]);
    println!("  Max: {:.2} ms", sorted[n - 1]);
}

#[tokio::main]
async fn main() {
    let _ = log4rs::init_config(default_log4rs_config()).unwrap();
    
    let program_name = std::env::args().next().unwrap_or_else(|| "benchcli".to_string());
    
    // Parse command line arguments
    let (client_name, burst_rate, config_file) = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error: {}", e);
            print_usage(&program_name);
            std::process::exit(1);
        }
    };
    
    // Read JSON configuration from file
    let config_json = match read_config(&config_file) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Error reading config file {}: {}", config_file, e);
            std::process::exit(1);
        }
    };
    
    // Parse the JSON to extract server_urls, server_threshold, and server_count
    let config_value: serde_json::Value = match serde_json::from_str(&config_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing JSON config: {}", e);
            std::process::exit(1);
        }
    };

    
    let server_urls: Vec<String> = config_value["server_urls"]
        .as_array()
        .ok_or_else(|| "server_urls must be an array".to_string())
        .and_then(|arr| {
            arr.iter()
                .map(|v| v.as_str().ok_or_else(|| "server_urls must contain strings".to_string()).map(|s| s.to_string()))
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_else(|e| {
            eprintln!("Error parsing server_urls: {}", e);
            std::process::exit(1);
        });
    
    let server_threshold = config_value["server_threshold"]
        .as_u64()
        .ok_or_else(|| "server_threshold must be a number".to_string())
        .unwrap_or_else(|e| {
            eprintln!("Error parsing server_threshold: {}", e);
            std::process::exit(1);
        }) as usize;
    
    let server_count = server_urls.len();
    
    // Store server URLs for later use in tasks
    let server_urls_clone = server_urls.clone();
    
    // Create a temporary share store file path
    let share_store_file_path = format!("/tmp/benchcli_{}_share_store.json", client_name);
    
    // Create client configuration
    let client_config = ClientConfig::new(
        client_name.clone(),
        server_threshold,
        server_count,
        server_urls,
        share_store_file_path.clone(),
    );

    // Copy the share store file for each task.
 
    // Create recovery client
    let mut client = RecoveryClient::new(client_config);
    
    // Password for operations
    let password = b"benchmark_password";
    let wrong_password = b"wrong_password";
    
    // First, do a store operation
    println!("Storing secret for initial setup...");
    match client.store(password, SECRET).await {
        Ok(()) => {
            println!("Secret stored successfully");
        }
        Err(e) => {
            eprintln!("Error storing secret: {}", e);
            std::process::exit(1);
        }
    }

    for task_id in 0..burst_rate {
        let task_client_name = format!("{}_task_{}", client_name, task_id);
        let task_share_store_path = format!("/tmp/benchcli_{}_share_store.json", task_client_name);
        let share_store_json = match std::fs::read_to_string(&share_store_file_path) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Error reading share store file: {}", e);
                std::process::exit(1);
            }
        };
        if let Err(e) = std::fs::write(&task_share_store_path, &share_store_json) {
            eprintln!("Error writing task share store file: {}", e);
            std::process::exit(1);
        }
    }
    
    
    // Collect all latencies
    let mut all_latencies = Vec::new();
    
    println!("Starting benchmark: {} iterations with burst rate {}", ITERATIONS, burst_rate);
    
    // Run iterations
    for iteration in 0..ITERATIONS {
        if iteration % 1000 == 0 && iteration > 0 {
            println!("Completed {} iterations...", iteration);
        }
        
        // Spawn burst_rate number of tasks
        let mut tasks = Vec::new();
        
        for task_id in 0..burst_rate {
            // Each task needs its own client instance
            // We'll create a new client for each task to avoid conflicts
            let task_client_name = format!("{}_task_{}", client_name, task_id);
            let task_share_store_path = format!("/tmp/benchcli_{}_share_store.json", task_client_name);

            // Create a new client config for this task
            let task_config = ClientConfig::new(
                client_name.clone(),
                server_threshold,
                server_count,
                server_urls_clone.clone(),
                task_share_store_path.clone(),
            );
            
            let mut task_client = RecoveryClient::new(task_config);
            let task = tokio::spawn(async move {
                
                // Measure latency for restore operation
                let start = Instant::now();
                let result = task_client.restore(wrong_password).await;
                let duration = start.elapsed();

                let latency_ms = duration.as_secs_f64() * 1000.0;

                latency_ms
                
                // match result {
                //     Ok(_) => {
                //         let latency_ms = duration.as_secs_f64() * 1000.0;
                //         Ok(latency_ms)
                //     }
                //     Err(e) => {
                //         eprintln!("Error in restore operation: {}", e);
                //         Err(e)
                //     }
                // }
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks to complete and collect latencies
        for task in tasks {
            match task.await {
                Ok(latency_ms) => {
                    all_latencies.push(latency_ms);
                }
                Err(e) => {
                    eprintln!("Task failed: {}", e);
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    
    println!("Benchmark completed. Total restore operations: {}", all_latencies.len());
    
    // Print CDF
    print_cdf(&all_latencies);
    
    // Cleanup temporary share store file
    let _ = std::fs::remove_file(&share_store_file_path);
}
