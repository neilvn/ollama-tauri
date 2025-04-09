// src-tauri/src/main.rs

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::Manager;
use std::error::Error;

mod proxy;

fn main() {
    env_logger::init();

    tauri::Builder::default()
        .setup(|app| {
            let app_handle = app.handle().clone();

            let app_data_dir = app_handle.path().app_data_dir()
                .map_err(|e| { // If it's an Err(e) variant...
                    // Log the specific Tauri error
                    log::error!("Failed to get app data directory: {}", e);
                    // Convert the error 'e' into the Box<dyn Error> type needed by setup
                    // (Requires 'e' to implement std::error::Error + Send + Sync)
                    Box::new(e) as Box<dyn Error + Send + Sync>
                }).unwrap();
                     // If Ok(path), unwraps path into app_data_dir.
                     // If Err(boxed_e), returns Err(boxed_e) from setup.

            // --- Construct Full DB Path ---
            let db_path = app_data_dir.join("responses.db");

            log::info!("Database path resolved to: {}", db_path.display());

            let db_path_clone = db_path.clone();

            // --- Spawn the proxy server task ---
            tauri::async_runtime::spawn(async move {
                log::info!("Spawning proxy server task...");
                if let Err(e) = proxy::run_proxy_server(db_path_clone).await {
                    log::error!("Proxy server exited with error: {}", e);
                } else {
                    log::info!("Proxy server task finished.");
                }
            });

            Ok(()) // Setup finished successfully
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|_app_handle, event| match event {
            tauri::RunEvent::ExitRequested { api, .. } => {
                api.prevent_exit();
            }
            _ => {}
        });
}
