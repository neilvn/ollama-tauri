use std::error::Error;
use tauri::Manager;
use tauri_plugin_log::Builder as LogPluginBuilder;

pub mod commands;
mod proxy;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(LogPluginBuilder::new().build())
        .invoke_handler(tauri::generate_handler![
            commands::greet,
            // Add any other commands here
        ])
        .setup(|app| {
            let app_handle = app.handle().clone();
            let app_data_dir = app_handle.path().app_data_dir()
                .map_err(|e| {
                    log::error!("Failed to get app data directory: {}", e);
                    Box::new(e) as Box<dyn Error + Send + Sync>
                }).unwrap();
            
            // Construct Full DB Path
            let db_path = app_data_dir.join("responses.db");
            log::info!("Database path resolved to: {}", db_path.display());
            let db_path_clone = db_path.clone();

            // log::info!("Using absolute database path: {}", db_path_clone.canonicalize()?.display());
            
            // Spawn the proxy server task
            tauri::async_runtime::spawn(async move {
                log::info!("Spawning proxy server task...");
                if let Err(e) = proxy::run_proxy_server(db_path_clone).await {
                    log::error!("Proxy server exited with error: {}", e);
                } else {
                    log::info!("Proxy server task finished.");
                }
            });
            
            Ok(())
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
