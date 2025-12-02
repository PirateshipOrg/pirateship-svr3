//! Client binary for SVR3 key recovery.
//! 
//! This binary provides a command-line interface for storing and restoring secrets
//! using the SVR3 protocol. It communicates with multiple servers via REST APIs.

use std::io::{self, Read, Write};
use log::info;
use pirateship_svr3::{
    client::RecoveryClient,
    log_config::default_log4rs_config,
};

/// Prints usage information for the client binary.
fn print_usage(program_name: &str) {
    eprintln!("Usage: {} <operation> <config_path> [options]", program_name);
    eprintln!();
    eprintln!("Operations:");
    eprintln!("  store    Store a secret using the SVR3 protocol");
    eprintln!("  restore  Restore a secret using the SVR3 protocol");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --password <password>     Password for OPRF operations (required)");
    eprintln!("  --secret-file <path>      Path to file containing secret to store (for store operation)");
    eprintln!("  --secret <secret>         Secret to store directly (for store operation)");
    eprintln!("  --output <path>           Path to write restored secret (for restore operation, default: stdout)");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} store config.json --password mypass --secret-file secret.bin", program_name);
    eprintln!("  {} store config.json --password mypass --secret 'my secret key'", program_name);
    eprintln!("  {} restore config.json --password mypass", program_name);
    eprintln!("  {} restore config.json --password mypass --output recovered.bin", program_name);
}

/// Reads a password from stdin.
/// 
/// Note: This function does not hide the password input for simplicity.
/// For production use, consider using a crate like `rpassword` for secure password input.
fn read_password() -> Result<String, io::Error> {
    print!("Enter password: ");
    io::stdout().flush()?;
    
    let mut password = String::new();
    io::stdin().read_line(&mut password)?;
    
    Ok(password.trim().to_string())
}

/// Reads secret from a file.
fn read_secret_file(path: &str) -> Result<Vec<u8>, io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut secret = Vec::new();
    file.read_to_end(&mut secret)?;
    Ok(secret)
}

/// Parses command line arguments.
fn parse_args() -> Result<(String, String, Option<String>, Option<String>, Option<String>, Option<String>), String> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 3 {
        return Err("Not enough arguments".to_string());
    }
    
    let operation = args[1].clone();
    let config_path = args[2].clone();
    
    if operation != "store" && operation != "restore" {
        return Err(format!("Invalid operation: {}. Must be 'store' or 'restore'", operation));
    }
    
    let mut password = None;
    let mut secret_file = None;
    let mut secret = None;
    let mut output = None;
    
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--password" => {
                if i + 1 >= args.len() {
                    return Err("--password requires a value".to_string());
                }
                password = Some(args[i + 1].clone());
                i += 2;
            }
            "--secret-file" => {
                if i + 1 >= args.len() {
                    return Err("--secret-file requires a path".to_string());
                }
                secret_file = Some(args[i + 1].clone());
                i += 2;
            }
            "--secret" => {
                if i + 1 >= args.len() {
                    return Err("--secret requires a value".to_string());
                }
                secret = Some(args[i + 1].clone());
                i += 2;
            }
            "--output" => {
                if i + 1 >= args.len() {
                    return Err("--output requires a path".to_string());
                }
                output = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                return Err(format!("Unknown argument: {}", args[i]));
            }
        }
    }
    
    Ok((operation, config_path, password, secret_file, secret, output))
}

#[tokio::main]
async fn main() {
    let _ = log4rs::init_config(default_log4rs_config()).unwrap();
    
    let program_name = std::env::args().next().unwrap_or_else(|| "client".to_string());
    
    // Parse command line arguments
    let (operation, config_path, password_arg, secret_file, secret_arg, output_path) = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error: {}", e);
            print_usage(&program_name);
            std::process::exit(1);
        }
    };
    
    // Get password
    let password = match password_arg {
        Some(p) => p,
        None => {
            // Try to read from stdin if not provided
            match read_password() {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Error reading password: {}", e);
                    std::process::exit(1);
                }
            }
        }
    };
    let password_bytes = password.as_bytes();
    
    // Load client configuration
    let config_json = match std::fs::read_to_string(&config_path) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Error reading config file {}: {}", config_path, e);
            std::process::exit(1);
        }
    };
    
    // Create recovery client
    let mut client = match RecoveryClient::from_json(&config_json) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Error parsing client config: {}", e);
            std::process::exit(1);
        }
    };
    
    // Perform the requested operation
    match operation.as_str() {
        "store" => {
            // Get secret to store
            let secret_bytes = if let Some(file_path) = secret_file {
                match read_secret_file(&file_path) {
                    Ok(secret) => secret,
                    Err(e) => {
                        eprintln!("Error reading secret file {}: {}", file_path, e);
                        std::process::exit(1);
                    }
                }
            } else if let Some(secret_str) = secret_arg {
                secret_str.as_bytes().to_vec()
            } else {
                eprintln!("Error: --secret-file or --secret must be provided for store operation");
                print_usage(&program_name);
                std::process::exit(1);
            };
            
            info!("Storing secret ({} bytes) with password...", secret_bytes.len());
            
            match client.store(password_bytes, &secret_bytes).await {
                Ok(()) => {
                    info!("Secret stored successfully");
                    println!("Secret stored successfully");
                }
                Err(e) => {
                    eprintln!("Error storing secret: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        "restore" => {
            info!("Restoring secret with password...");
            
            let restored_secret = match client.restore(password_bytes).await {
                Ok(secret) => secret,
                Err(e) => {
                    eprintln!("Error restoring secret: {}", e);
                    std::process::exit(1);
                }
            };
            
            info!("Secret restored successfully ({} bytes)", restored_secret.len());
            
            // Write the restored secret to output
            if let Some(output_file) = output_path {
                match std::fs::write(&output_file, &restored_secret) {
                    Ok(()) => {
                        info!("Secret written to {}", output_file);
                        println!("Secret restored and written to {}", output_file);
                    }
                    Err(e) => {
                        eprintln!("Error writing secret to {}: {}", output_file, e);
                        std::process::exit(1);
                    }
                }
            } else {
                // Write to stdout
                match io::stdout().write_all(&restored_secret) {
                    Ok(()) => {
                        // Don't print success message if writing to stdout (might be binary)
                    }
                    Err(e) => {
                        eprintln!("Error writing secret to stdout: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        
        _ => {
            eprintln!("Invalid operation: {}", operation);
            print_usage(&program_name);
            std::process::exit(1);
        }
    }
}

