import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("036_ProjectionThreadSessionRetrying", (it) => {
  it.effect("adds a retrying flag with a safe false default", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* runMigrations({ toMigrationInclusive: 36 });

      const columns = yield* sql<{
        readonly name: string;
        readonly dflt_value: string | null;
      }>`
        PRAGMA table_info(projection_thread_sessions)
      `;
      const retrying = columns.find((column) => column.name === "retrying");
      assert.ok(retrying);
      assert.strictEqual(retrying.dflt_value, "0");
    }),
  );
});
