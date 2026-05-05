// Fixture: TypeORM migration with both SQL-string and table-method evidence.
//
// Anchors the extraction fidelity for TypeORM migrations:
// - `migration_primary_symbol` should pick the `up` function (preferred) or
//   the `*Migration` class.
// - `query_runner_sql_literals` should extract `alerts` and `notifications`
//   from the SQL strings inside `queryRunner.query(...)`.
// - The typed call-site path should extract `audit_logs` from the
//   `queryRunner.addColumn(...)` call.
// - A non-TypeORM substring-of-Migration class (`DataMigrationHelper`) must
//   NOT become the primary symbol — the `up` function takes precedence and,
//   even without `up`, only classes whose name *ends with* `Migration` count.

import { MigrationInterface, QueryRunner } from 'typeorm';

class DataMigrationHelper {
  static normalize(name: string): string {
    return name.trim().toLowerCase();
  }
}

export class AddAlertWorkflow1714410000000 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "alerts" ADD COLUMN "workflow" jsonb NOT NULL DEFAULT '{}'`,
    );
    await queryRunner.query(
      `ALTER TABLE "notifications" ADD COLUMN "workflow_id" uuid`,
    );
    await queryRunner.addColumn('audit_logs', {
      name: 'workflow_id',
      type: 'uuid',
      isNullable: true,
    } as never);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropColumn('audit_logs', 'workflow_id');
    await queryRunner.query(`DROP INDEX "idx_notifications_workflow_id"`);
    await queryRunner.query(
      `ALTER TABLE "alerts" DROP COLUMN "workflow"`,
    );
  }
}
