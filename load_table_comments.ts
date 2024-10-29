import * as fs from "fs";
import * as path from "path";
import { createClient } from "@clickhouse/client";

const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL;
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD;

if (!CLICKHOUSE_URL || !CLICKHOUSE_USER || !CLICKHOUSE_PASSWORD) {
  console.error("Environment variables CLICKHOUSE_URL, CLICKHOUSE_USER, and CLICKHOUSE_PASSWORD must be set");
  process.exit(1);
}

const client = createClient({
  url: CLICKHOUSE_URL,
  username: CLICKHOUSE_USER,
  password: CLICKHOUSE_PASSWORD,
});

interface Table {
  database: string;
  table: string;
  query?: string;
  comment?: string;
}

const loadTableComments = async () => {
  const filePath = path.resolve(__dirname, "table_comments.json");

  fs.readFile(filePath, "utf8", async (err, data) => {
    if (err) {
      console.error("Error reading file:", err);
      return;
    }

    try {
      const tables = JSON.parse(data) as { tables: Table[] };

      // Create the temporary table
      await client.exec({
        query: `
          CREATE TABLE IF NOT EXISTS default.tables_temp
          (
              database LowCardinality(String),
              table LowCardinality(String),
              query String DEFAULT 'SELECT * FROM ' || database || '.' || table || ' LIMIT 100',
              comment String DEFAULT database || '.'|| table
          )
          ORDER BY (database,table)
        `,
      });
      console.log("Created temporary table tables_temp");

      // Insert data into the temporary table
      for (const table of tables.tables) {
        const row: Table = {
          database: table.database,
          table: table.table,
          query: table.query,
          comment: table.comment
        };

        await client.insert({
          table: "default.tables_temp",
          values: [row],
          format: 'JSONEachRow',
        });
        console.log(`Inserted table: ${table.database}.${table.table} into tables_temp`);
      }

      // Swap tables
      await client.exec({ query: `EXCHANGE TABLES default.tables_temp AND default.tables` });
      console.log("Swapped tables tables_temp and tables");

      // Drop the temporary table
      await client.exec({ query: `DROP TABLE default.tables_temp` });
      console.log("Dropped temporary table tables_temp");

    } catch (error: unknown) {
      console.log(error)
    } finally {
      await client.close();
    }
  });
};

loadTableComments();