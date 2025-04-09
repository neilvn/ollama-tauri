
#[tauri::command]
pub fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
pub fn goodbye(name: &str) -> String {
    format!("Goodbye, {}!", name)
}

