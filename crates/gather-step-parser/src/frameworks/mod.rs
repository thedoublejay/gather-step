pub mod azure;
pub mod detect;
pub mod drizzle;
pub mod frontend_hooks;
pub mod frontend_react;
pub mod frontend_router;
pub mod gateway_proxy;
pub mod local_config;
pub mod mongoose;
pub mod mongoose_migration;
pub mod nestjs;
pub mod nextjs;
pub mod prisma;
pub mod profile;
pub mod registry;
pub mod storybook;
pub mod tailwind;

pub use detect::{Framework, detect_frameworks};
