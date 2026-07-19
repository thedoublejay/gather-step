pub mod ai_typescript;
pub mod azure;
pub mod detect;
pub mod drizzle;
pub mod fastapi;
pub mod frontend_hooks;
pub mod frontend_react;
pub mod frontend_router;
pub mod gateway_proxy;
pub mod http_client;
pub mod local_config;
pub mod migration_utils;
pub mod mongo;
pub mod mongo_safety;
pub mod mongoose;
pub mod mongoose_migration;
pub mod nestjs;
pub mod nextjs;
pub mod prisma;
pub mod profile;
pub mod python_http;
pub mod python_kafka;
pub mod python_payload;
pub mod registry;
pub mod storybook;
pub mod tailwind;
pub mod typeorm;
pub mod typeorm_migration;

pub use detect::{Framework, detect_frameworks, detect_frameworks_workspace_aware};

/// Compose two route segments into a single normalised path, collapsing
/// surrounding slashes and quotes. Shared by the route-emitting framework
/// passes (`NestJS` controllers, `FastAPI` router prefixes).
pub fn join_route_path(base: &str, method_path: &str) -> String {
    let mut pieces = Vec::new();
    for piece in [base, method_path] {
        let trimmed = piece
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches('/');
        if !trimmed.is_empty() {
            pieces.push(trimmed);
        }
    }
    if pieces.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", pieces.join("/"))
    }
}
