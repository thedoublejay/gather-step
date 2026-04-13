use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_mcp::budget::BudgetWriter;
use serde::Serialize;

#[derive(Serialize)]
struct BenchPayload<'a> {
    values: &'a [&'a str],
}

fn bench_budget_writer(c: &mut Criterion) {
    let payload = BenchPayload {
        values: &[
            "backend_standard/src/controller.ts",
            "backend_standard/src/order.service.ts",
            "frontend_standard/src/consumer.ts",
            "shared_contracts/src/order.ts",
        ],
    };

    c.bench_function("budget_writer_serialize", |b| {
        b.iter(|| {
            let mut writer = BudgetWriter::new(None);
            serde_json::to_writer(&mut writer, &payload).expect("payload should serialize");
            writer.bytes_written()
        });
    });
}

criterion_group!(benches, bench_budget_writer);
criterion_main!(benches);
