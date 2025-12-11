# データエンジニアリング入門 → Apache Iceberg 習得カリキュラム

Webエンジニア向けに、データエンジニアリングの基礎からApache Icebergの実践までを段階的に学ぶカリキュラムです。

**使用言語**: TypeScript（Node.js）

## 前提環境

```bash
# Node.js 22以上推奨（24.x LTSも可）
node -v

# プロジェクト初期化
mkdir data-engineering-learning
cd data-engineering-learning
pnpm init
pnpm add -D typescript ts-node @types/node
pnpm exec tsc --init

# 必要なパッケージ（Week毎に追加）
pnpm add @duckdb/node-api          # 旧duckdbパッケージは非推奨
pnpm add pg @types/pg              # PostgreSQL
```

> **重要**: 旧 `duckdb` パッケージは非推奨となり、DuckDB 1.5.x（2026年初頭）以降はリリースされません。
> 新しい `@duckdb/node-api`（Node Neo）を使用してください。

## 全体像

```
Week 1: データエンジニアリングの基礎概念
    ↓
Week 2: 列指向フォーマットとParquet
    ↓
Week 3: DuckDBによる分析クエリ
    ↓
Week 4-5: Apache Icebergの実践
```

---

## Week 1: データエンジニアリングの基礎概念

### 1-1: OLTPとOLAPの違い（座学 + 簡単な実験）

#### Webエンジニアの世界（OLTP）

```
PostgreSQL / MySQL
├── 1行単位の読み書きが高速
├── トランザクション重視
├── 正規化されたスキーマ
└── 数GB〜数百GB規模
```

#### データエンジニアの世界（OLAP）

```
BigQuery / Redshift / DuckDB
├── 大量データの集計が高速
├── 分析クエリ重視
├── 非正規化・スター/スノーフレークスキーマ
└── 数TB〜数PB規模
```

#### 課題1-1: PostgreSQLで体感する

```typescript
// src/week1/oltp-vs-olap.ts
import { Client } from "pg";

async function main() {
  const client = new Client({
    host: "localhost",
    port: 5432,
    database: "testdb",
    user: "postgres",
    password: "postgres",
  });

  await client.connect();

  // テーブル作成
  await client.query(`
    DROP TABLE IF EXISTS access_logs;
    CREATE TABLE access_logs (
      id SERIAL PRIMARY KEY,
      user_id INT,
      path VARCHAR(255),
      status_code INT,
      response_time_ms INT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // 100万行のダミーデータ投入
  console.log("Inserting 1,000,000 rows...");
  console.time("insert");

  await client.query(`
    INSERT INTO access_logs (user_id, path, status_code, response_time_ms, created_at)
    SELECT 
      (random() * 10000)::int,
      '/api/v1/resource/' || (random() * 100)::int,
      CASE WHEN random() < 0.95 THEN 200 ELSE 500 END,
      (random() * 1000)::int,
      NOW() - (random() * interval '365 days')
    FROM generate_series(1, 1000000);
  `);
  console.timeEnd("insert");

  // OLTPクエリ（高速）- 主キー検索
  console.log("\n=== OLTP Query (Primary Key Lookup) ===");
  console.time("oltp");
  const oltpResult = await client.query(
    "SELECT * FROM access_logs WHERE id = 500000"
  );
  console.timeEnd("oltp");
  console.log("Rows:", oltpResult.rowCount);

  // OLAPクエリ（遅い）- 集計
  console.log("\n=== OLAP Query (Aggregation) ===");
  console.time("olap");
  const olapResult = await client.query(`
    SELECT 
      DATE_TRUNC('month', created_at) as month,
      COUNT(*) as count,
      AVG(response_time_ms) as avg_response_time
    FROM access_logs
    GROUP BY 1
    ORDER BY 1
  `);
  console.timeEnd("olap");
  console.log("Result rows:", olapResult.rowCount);

  await client.end();
}

main().catch(console.error);
```

**確認ポイント**: なぜ集計クエリが遅いのか？（全行スキャンが必要）

---

### 1-2: 行指向 vs 列指向ストレージ

#### 行指向（PostgreSQL、MySQL）

```
Row 1: [id=1, user_id=100, path="/api", status=200, time=50]
Row 2: [id=2, user_id=101, path="/api", status=200, time=45]
Row 3: [id=3, user_id=100, path="/web", status=500, time=120]
```

→ 1行取得は高速、特定カラムだけの集計は全データ読み込み

#### 列指向（Parquet、DuckDB）

```
Column "id":        [1, 2, 3, ...]
Column "user_id":   [100, 101, 100, ...]
Column "status":    [200, 200, 500, ...]
Column "time":      [50, 45, 120, ...]
```

→ 必要なカラムだけ読める、圧縮効率が高い

#### 課題1-2: CSVとParquetのサイズ比較

```typescript
// src/week1/csv-vs-parquet.ts
import { DuckDBInstance } from "@duckdb/node-api";
import * as fs from "fs";

async function main() {
  const db = await DuckDBInstance.create(":memory:");
  const conn = await db.connect();

  const dataDir = "./data";
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // 100万行のデータを生成してCSVとParquetで保存
  console.log("Generating 1,000,000 rows...");

  await conn.run(`
    CREATE TABLE events AS
    SELECT
      (random() * 10000)::INT as user_id,
      CASE (random() * 3)::INT
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'view'
        ELSE 'purchase'
      END as event_type,
      random() * 1000 as amount,
      TIMESTAMP '2024-01-01' + INTERVAL (random() * 365) DAY as timestamp
    FROM generate_series(1, 1000000);
  `);

  // CSVで保存
  console.log("\nExporting to CSV...");
  await conn.run(`COPY events TO '${dataDir}/data.csv' (HEADER, DELIMITER ',');`);

  // Parquetで保存
  console.log("Exporting to Parquet...");
  await conn.run(`COPY events TO '${dataDir}/data.parquet' (FORMAT PARQUET);`);

  // ファイルサイズ比較
  const csvSize = fs.statSync(`${dataDir}/data.csv`).size;
  const parquetSize = fs.statSync(`${dataDir}/data.parquet`).size;

  console.log("\n=== File Size Comparison ===");
  console.log(`CSV:     ${(csvSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Parquet: ${(parquetSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Ratio:   ${(csvSize / parquetSize).toFixed(2)}x smaller`);

  await conn.close();
  await db.close();
}

main().catch(console.error);
```

**確認ポイント**: Parquetが圧倒的に小さい理由を理解する

---

### 1-3: データレイクとデータウェアハウスとデータレイクハウス

```
┌─────────────────────────────────────────────────────────────┐
│                     データソース                              │
│  (Webアプリ DB, ログ, 外部API, IoT, etc.)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    データレイク                               │
│  ・S3/GCSなどのオブジェクトストレージ                          │
│  ・生データをそのまま保存（JSON, CSV, Parquet）                │
│  ・スキーマは後から定義（Schema on Read）                      │
│  ・安価だが、クエリは遅い                                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 データウェアハウス                             │
│  ・BigQuery, Redshift, Snowflake                            │
│  ・構造化・最適化されたデータ                                  │
│  ・高速なクエリ                                               │
│  ・高価                                                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              データレイクハウス（Iceberg等）                   │
│  ・データレイクの安さ + DWHの機能性                            │
│  ・オブジェクトストレージ上でACID/スキーマ進化                  │
│  ・複数エンジンからアクセス可能                                │
└─────────────────────────────────────────────────────────────┘
```

#### 課題1-3: 概念整理ドキュメント作成

以下の質問に自分の言葉で回答するドキュメントを作成：

1. あなたのWebアプリのログデータ（1日1GB）を5年間保存する場合、RDBに入れ続けるとどんな問題が起きるか？
2. データレイクに保存する場合のメリット・デメリットは？
3. なぜ「データレイクハウス」という概念が生まれたか？

---

## Week 2: Parquetとファイルベースのデータ管理

### 2-1: Parquetの内部構造

```
┌────────────────────────────────────────┐
│            Parquetファイル              │
├────────────────────────────────────────┤
│  Row Group 1                           │
│    ├── Column Chunk: user_id           │
│    │     └── Page (圧縮済みデータ)      │
│    ├── Column Chunk: event_type        │
│    └── Column Chunk: amount            │
├────────────────────────────────────────┤
│  Row Group 2                           │
│    └── ...                             │
├────────────────────────────────────────┤
│  Footer (メタデータ)                    │
│    ├── スキーマ情報                     │
│    ├── Row Group位置                   │
│    └── 統計情報（min/max/count）        │
└────────────────────────────────────────┘
```

#### 課題2-1: Parquetメタデータの確認

```typescript
// src/week2/parquet-metadata.ts
import { DuckDBInstance } from "@duckdb/node-api";

async function main() {
  const db = await DuckDBInstance.create(":memory:");
  const conn = await db.connect();

  console.log("=== Parquet Metadata ===\n");

  // スキーマ確認
  console.log("Schema:");
  const schemaReader = await conn.runAndReadAll(
    `DESCRIBE SELECT * FROM './data/data.parquet'`
  );
  console.table(schemaReader.getRows());

  // Parquetメタデータの詳細
  console.log("\nFile Metadata:");
  const metaReader = await conn.runAndReadAll(
    `SELECT * FROM parquet_metadata('./data/data.parquet')`
  );
  console.table(metaReader.getRows());

  // Row Groupごとの統計情報
  console.log("\nColumn Statistics per Row Group:");
  const statsReader = await conn.runAndReadAll(`
    SELECT
      row_group_id,
      column_id,
      path_in_schema as column_name,
      num_values,
      stats_min,
      stats_max
    FROM parquet_metadata('./data/data.parquet')
  `);
  console.table(statsReader.getRows());

  await conn.close();
  await db.close();
}

main().catch(console.error);
```

**確認ポイント**: 統計情報があることで、クエリ時に不要なRow Groupをスキップできる

---

### 2-2: パーティショニングの基礎

#### 課題2-2: パーティション分割の実践

```typescript
// src/week2/partitioning.ts
import { DuckDBInstance } from "@duckdb/node-api";
import * as fs from "fs";
import * as path from "path";

async function main() {
  const db = await DuckDBInstance.create(":memory:");
  const conn = await db.connect();

  const partitionedDir = "./data/partitioned_data";
  if (fs.existsSync(partitionedDir)) {
    fs.rmSync(partitionedDir, { recursive: true });
  }

  // 1年分のデータを作成
  console.log("Creating 1 year of event data...");

  await conn.run(`
    CREATE TABLE events AS
    SELECT
      TIMESTAMP '2024-01-01' + INTERVAL (i / 1000) DAY as event_date,
      (random() * 10000)::INT as user_id,
      CASE (random() * 3)::INT
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'view'
        ELSE 'purchase'
      END as event_type,
      random() * 1000 as amount
    FROM generate_series(1, 365000) as t(i);
  `);

  // 年月でパーティション分割して保存
  console.log("\nExporting with partitioning by year/month...");

  await conn.run(`
    COPY (
      SELECT
        *,
        YEAR(event_date) as year,
        MONTH(event_date) as month
      FROM events
    ) TO '${partitionedDir}'
    (FORMAT PARQUET, PARTITION_BY (year, month));
  `);

  // ディレクトリ構造を表示
  console.log("\n=== Directory Structure ===");
  listDir(partitionedDir);

  // パーティションプルーニングの効果を確認
  console.log("\n=== Query with Partition Pruning ===");

  console.log("\nFull scan (all months):");
  console.time("full-scan");
  const fullReader = await conn.runAndReadAll(
    `SELECT COUNT(*), AVG(amount) FROM '${partitionedDir}/**/*.parquet'`
  );
  console.table(fullReader.getRows());
  console.timeEnd("full-scan");

  console.log("\nPartition pruning (January only):");
  console.time("partition-pruning");
  const partReader = await conn.runAndReadAll(
    `SELECT COUNT(*), AVG(amount) FROM '${partitionedDir}/year=2024/month=1/*.parquet'`
  );
  console.table(partReader.getRows());
  console.timeEnd("partition-pruning");

  await conn.close();
  await db.close();
}

function listDir(dir: string, indent = "") {
  const items = fs.readdirSync(dir);
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      console.log(`${indent}dir: ${item}/`);
      listDir(fullPath, indent + "  ");
    } else {
      const sizeKB = (stat.size / 1024).toFixed(1);
      console.log(`${indent}file: ${item} (${sizeKB} KB)`);
    }
  }
}

main().catch(console.error);
```

**確認ポイント**: 

- `year=2024/month=1/` のようなディレクトリ構造
- 特定月のクエリ時に他の月のファイルを読まなくて済む

---

### 2-3: Hive形式の限界（Icebergが解決する問題）

#### 課題2-3: Hive形式の問題を体験

```typescript
// src/week2/hive-limitations.ts
import * as fs from "fs";
import * as path from "path";

async function main() {
  const partitionedDir = "./data/partitioned_data";

  console.log("=== Hive Format Limitations ===\n");

  // 問題1: ファイル一覧取得のコスト
  console.log("Problem 1: File listing cost");
  console.time("file-listing");
  let fileCount = 0;
  function countFiles(dir: string) {
    const items = fs.readdirSync(dir);
    for (const item of items) {
      const fullPath = path.join(dir, item);
      const stat = fs.statSync(fullPath);
      if (stat.isDirectory()) {
        countFiles(fullPath);
      } else if (item.endsWith(".parquet")) {
        fileCount++;
      }
    }
  }
  countFiles(partitionedDir);
  console.timeEnd("file-listing");
  console.log(`Total files: ${fileCount}`);
  console.log("→ パーティションが増えるとこの処理が重くなる\n");

  // 問題2: 更新の難しさ
  console.log("Problem 2: Difficulty of updates");
  console.log(`
  特定レコードを更新するには？
  
  1. 該当パーティションの全データを読み込む
  2. メモリ上で更新
  3. パーティション全体を書き直す
  
  → 1レコード更新でも大量のI/Oが発生
  → 同時更新時の整合性は自己管理
  `);

  // 問題3: スキーマ変更
  console.log("Problem 3: Schema evolution");
  console.log(`
  カラムを追加したら？
  
  - 既存ファイルには新カラムがない
  - 新旧ファイルの整合性は自己管理
  - 型変更は実質不可能（全ファイル書き換え必要）
  `);

  // 問題4: トランザクションがない
  console.log("Problem 4: No ACID transactions");
  console.log(`
  書き込み中にクエリが来たら？
  
  - 中途半端な状態のデータが見える可能性
  - ロールバック機構がない
  - 複数ファイルへの書き込みがアトミックでない
  `);

  console.log("\n=== These problems are what Iceberg solves ===");
  console.log(`
  Iceberg provides:
  ✓ Snapshot isolation (ACID)
  ✓ Row-level updates/deletes
  ✓ Schema evolution without rewriting
  ✓ Partition evolution
  ✓ Time travel
  ✓ Efficient metadata management
  `);
}

main().catch(console.error);
```

**まとめドキュメント作成**: Hive形式の限界を3つ挙げ、それぞれがどんな運用問題を引き起こすか記述

---

## Week 3: DuckDBによる分析クエリ

### 3-1: DuckDBの概念理解

```
┌─────────────────────────────────────────────────────────────┐
│                        DuckDB                               │
│  ・組み込み型の列指向OLAP DB（SQLiteのOLAP版）               │
│  ・インストール不要、単一ファイル                             │
│  ・Parquet/CSV/JSONを直接クエリ可能                          │
│  ・Node.js/Python/Rust等から利用可能                         │
└─────────────────────────────────────────────────────────────┘
```

**Webエンジニア向けの例え**:

- SQLite = 組み込みOLTP DB（1行の読み書きが得意）
- DuckDB = 組み込みOLAP DB（集計・分析が得意）

---

### 3-2: DuckDB基礎

#### 課題3-2: DuckDBでParquetを操作

```typescript
// src/week3/duckdb-basics.ts
import { DuckDBInstance } from "@duckdb/node-api";

async function main() {
  const db = await DuckDBInstance.create(":memory:");
  const conn = await db.connect();

  console.log("=== DuckDB Basics ===\n");

  // Parquetファイルを直接クエリ（テーブル作成不要）
  console.log("1. Query Parquet directly:");
  const r1 = await conn.runAndReadAll(
    `SELECT * FROM './data/data.parquet' LIMIT 5`
  );
  console.table(r1.getRows());

  // SQLでの集計
  console.log("\n2. Aggregation:");
  const r2 = await conn.runAndReadAll(`
    SELECT
      event_type,
      COUNT(*) as count,
      ROUND(AVG(amount), 2) as avg_amount,
      ROUND(SUM(amount), 2) as total_amount
    FROM './data/data.parquet'
    GROUP BY event_type
    ORDER BY count DESC
  `);
  console.table(r2.getRows());

  // 時系列分析
  console.log("\n3. Time series analysis:");
  const r3 = await conn.runAndReadAll(`
    SELECT
      DATE_TRUNC('month', timestamp) as month,
      COUNT(*) as events,
      ROUND(SUM(amount), 2) as revenue
    FROM './data/data.parquet'
    GROUP BY 1
    ORDER BY 1
    LIMIT 6
  `);
  console.table(r3.getRows());

  // Window関数
  console.log("\n4. Window functions (running total):");
  const r4 = await conn.runAndReadAll(`
    WITH monthly AS (
      SELECT
        DATE_TRUNC('month', timestamp) as month,
        SUM(amount) as revenue
      FROM './data/data.parquet'
      GROUP BY 1
    )
    SELECT
      month,
      ROUND(revenue, 2) as revenue,
      ROUND(SUM(revenue) OVER (ORDER BY month), 2) as cumulative_revenue
    FROM monthly
    ORDER BY month
    LIMIT 6
  `);
  console.table(r4.getRows());

  // 複数ファイルの結合
  console.log("\n5. Query partitioned data with glob:");
  const r5 = await conn.runAndReadAll(`
    SELECT
      year, month, COUNT(*) as events
    FROM './data/partitioned_data/**/*.parquet'
    GROUP BY year, month
    ORDER BY year, month
    LIMIT 6
  `);
  console.table(r5.getRows());

  await conn.close();
  await db.close();
}

main().catch(console.error);
```

---

### 3-3: 実行計画の読み方

#### 課題3-3: EXPLAINで処理を理解

```typescript
// src/week3/explain-plan.ts
import { DuckDBInstance } from "@duckdb/node-api";

async function main() {
  const db = await DuckDBInstance.create(":memory:");
  const conn = await db.connect();

  console.log("=== Query Execution Plans ===\n");

  // シンプルなクエリ
  console.log("1. Simple aggregation plan:");
  const r1 = await conn.runAndReadAll(`
    EXPLAIN ANALYZE
    SELECT event_type, SUM(amount)
    FROM './data/data.parquet'
    WHERE amount > 100
    GROUP BY event_type
  `);
  const rows1 = r1.getRows();
  if (rows1.length > 0) {
    console.log(rows1[0]);
  }

  // フィルタ付きクエリ
  console.log("\n2. With predicate pushdown:");
  const r2 = await conn.runAndReadAll(`
    EXPLAIN ANALYZE
    SELECT COUNT(*)
    FROM './data/data.parquet'
    WHERE user_id = 1234
  `);
  const rows2 = r2.getRows();
  if (rows2.length > 0) {
    console.log(rows2[0]);
  }

  await conn.close();
  await db.close();
}

main().catch(console.error);
```

**確認ポイント**:

- PARQUET_SCAN（ファイル読み込み）
- FILTER（フィルタリング）
- HASH_GROUP_BY（集計）
- Predicate Pushdown（フィルタのプッシュダウン）

---

### 3-4: DuckDBとPostgreSQLのパフォーマンス比較

#### 課題3-4: OLTP vs OLAP実測

```typescript
// src/week3/performance-comparison.ts
import { DuckDBInstance } from "@duckdb/node-api";
import { Client } from "pg";

async function main() {
  console.log("=== OLTP (PostgreSQL) vs OLAP (DuckDB) Performance ===\n");

  const pgClient = new Client({
    host: "localhost",
    port: 5432,
    database: "testdb",
    user: "postgres",
    password: "postgres",
  });
  await pgClient.connect();

  const db = await DuckDBInstance.create(":memory:");
  const duckConn = await db.connect();

  const rowCount = 1000000;
  console.log(`Loading ${rowCount.toLocaleString()} rows into both databases...\n`);

  // PostgreSQLにデータ投入
  await pgClient.query(`DROP TABLE IF EXISTS events`);
  await pgClient.query(`
    CREATE TABLE events (
      id SERIAL PRIMARY KEY,
      user_id INT,
      event_type VARCHAR(20),
      amount DECIMAL(10,2),
      created_at TIMESTAMP
    )
  `);
  await pgClient.query(`
    INSERT INTO events (user_id, event_type, amount, created_at)
    SELECT
      (random() * 10000)::int,
      CASE (random() * 3)::int WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'purchase' END,
      random() * 1000,
      NOW() - (random() * interval '365 days')
    FROM generate_series(1, ${rowCount})
  `);

  // DuckDBにデータ投入
  await duckConn.run(`
    CREATE TABLE events AS
    SELECT
      i as id,
      (random() * 10000)::INT as user_id,
      CASE (random() * 3)::INT WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'purchase' END as event_type,
      random() * 1000 as amount,
      TIMESTAMP '2024-01-01' + INTERVAL (random() * 365) DAY as created_at
    FROM generate_series(1, ${rowCount}) as t(i)
  `);

  // ベンチマーク1: 主キー検索
  console.log("--- Benchmark 1: Primary Key Lookup (OLTP) ---");
  console.time("PostgreSQL");
  await pgClient.query("SELECT * FROM events WHERE id = 500000");
  console.timeEnd("PostgreSQL");

  console.time("DuckDB");
  await duckConn.run("SELECT * FROM events WHERE id = 500000");
  console.timeEnd("DuckDB");
  console.log("-> PostgreSQL wins (indexed lookup)\n");

  // ベンチマーク2: 集計クエリ
  console.log("--- Benchmark 2: Aggregation (OLAP) ---");
  console.time("PostgreSQL");
  await pgClient.query(`
    SELECT event_type, COUNT(*), AVG(amount)
    FROM events
    GROUP BY event_type
  `);
  console.timeEnd("PostgreSQL");

  console.time("DuckDB");
  await duckConn.run(`
    SELECT event_type, COUNT(*), AVG(amount)
    FROM events
    GROUP BY event_type
  `);
  console.timeEnd("DuckDB");
  console.log("-> DuckDB wins (columnar scan)\n");

  // ベンチマーク3: 複雑な集計
  console.log("--- Benchmark 3: Complex Aggregation ---");
  const complexQuery = `
    SELECT
      DATE_TRUNC('month', created_at) as month,
      event_type,
      COUNT(*) as count,
      SUM(amount) as total,
      AVG(amount) as avg
    FROM events
    GROUP BY 1, 2
    ORDER BY 1, 2
  `;

  console.time("PostgreSQL");
  await pgClient.query(complexQuery);
  console.timeEnd("PostgreSQL");

  console.time("DuckDB");
  await duckConn.run(complexQuery);
  console.timeEnd("DuckDB");
  console.log("-> DuckDB wins significantly\n");

  await pgClient.end();
  await duckConn.close();
  await db.close();
}

main().catch(console.error);
```

---

## Week 4-5: Apache Iceberg実践

> **Note**: Icebergの操作には現時点でSpark/Trino/Flinkなどが必要です。
> TypeScriptからはREST Catalog APIを通じてメタデータ操作が可能ですが、
> データ操作の実践はSpark SQLを使用します。

### 4-1: Iceberg環境構築

> **Note**: Spark 4.0 と Iceberg 1.10.0 を使用します。

```yaml
# docker-compose.yml
services:
  spark-iceberg:
    build: ./spark
    container_name: spark-iceberg
    ports:
      - "8888:8888"  # Jupyter
      - "8080:8080"  # Spark UI
      - "10000:10000"
      - "10001:10001"
    volumes:
      - ./warehouse:/home/iceberg/warehouse
      - ./notebooks:/home/iceberg/notebooks
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    depends_on:
      - rest
      - minio

  rest:
    image: apache/iceberg-rest-fixture
    container_name: iceberg-rest
    ports:
      - "8181:8181"
    environment:
      - CATALOG_WAREHOUSE=s3://warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000
      - CATALOG_S3_PATH__STYLE__ACCESS=true
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1

  minio:
    image: minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
    command: server /data --console-address ":9001"

  mc:
    image: minio/mc
    container_name: mc
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      sleep 5;
      mc alias set minio http://minio:9000 admin password;
      mc mb minio/warehouse;
      exit 0;
      "
```

```dockerfile
# spark/Dockerfile
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root

# Iceberg runtime JARs
RUN curl -o /opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar && \
    curl -o /opt/spark/jars/iceberg-aws-bundle-1.10.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar

# Python dependencies
RUN pip install jupyterlab "pyiceberg[s3fs,pyarrow]>=0.10.0"

WORKDIR /home/iceberg
USER spark

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
```

```bash
# 起動
docker-compose up -d

# Jupyter Notebookが http://localhost:8888 で起動
# MinIO Consoleが http://localhost:9001 で起動（admin/password）
```

#### Spark 4.0の主な変更点

1. **ANSIモードがデフォルト有効** - 厳密なSQL動作（暗黙の型変換制限など）
2. **VARIANT型ネイティブサポート** - Iceberg V3のVARIANT型とシームレスに連携
3. **Spark Connectの強化** - 軽量Pythonクライアント（pyspark-client: 1.5MB）

---

### 4-2: テーブル作成とCRUD操作

**シナリオ**: ECサイトの注文データを管理する

```sql
-- テーブル作成
CREATE TABLE demo.db.orders (
    order_id BIGINT,
    customer_id BIGINT,
    product_name STRING,
    amount DECIMAL(10, 2),
    order_date DATE,
    created_at TIMESTAMP
) USING iceberg;

-- UPDATE例
UPDATE demo.db.orders
SET amount = 1500.00
WHERE order_id = 1;

-- DELETE例
DELETE FROM demo.db.orders
WHERE order_id = 5;

-- MERGE INTO例
MERGE INTO demo.db.orders t
USING updates s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

### 4-3: スキーマ進化

```sql
-- カラム追加
ALTER TABLE demo.db.orders ADD COLUMN discount_rate DOUBLE;

-- カラムリネーム
ALTER TABLE demo.db.orders RENAME COLUMN product_name TO item_name;

-- スキーマ変更履歴確認
SELECT * FROM demo.db.orders.history;
```

---

### 4-4: パーティション進化

```sql
-- パーティション追加
ALTER TABLE demo.db.orders
ADD PARTITION FIELD month(order_date);

-- パーティション構造確認
SELECT * FROM demo.db.orders.partitions;
```

---

### 4-5: タイムトラベルとロールバック

```sql
-- スナップショット一覧
SELECT * FROM demo.db.orders.snapshots;

-- 過去時点参照
SELECT * FROM demo.db.orders VERSION AS OF <snapshot_id>;

-- タイムスタンプ指定
SELECT * FROM demo.db.orders TIMESTAMP AS OF '2024-01-15 10:00:00';

-- ロールバック
CALL demo.system.rollback_to_snapshot('db.orders', <snapshot_id>);
```

---

### 4-6: メンテナンス操作

```sql
-- Compaction
CALL demo.system.rewrite_data_files('db.orders');

-- 古いスナップショット削除
CALL demo.system.expire_snapshots('db.orders', TIMESTAMP '2024-01-01 00:00:00');

-- 孤立ファイル削除
CALL demo.system.remove_orphan_files('db.orders');
```

---

### 4-7: TypeScriptからIceberg REST Catalogにアクセス（発展）

```typescript
// src/week4/iceberg-rest-catalog.ts
interface IcebergCatalogConfig {
  baseUrl: string;
  warehouse: string;
}

class IcebergRestClient {
  private config: IcebergCatalogConfig;

  constructor(config: IcebergCatalogConfig) {
    this.config = config;
  }

  async listNamespaces(): Promise<string[]> {
    const response = await fetch(`${this.config.baseUrl}/v1/namespaces`);
    const data = await response.json();
    return data.namespaces;
  }

  async listTables(namespace: string): Promise<string[]> {
    const response = await fetch(
      `${this.config.baseUrl}/v1/namespaces/${namespace}/tables`
    );
    const data = await response.json();
    return data.identifiers.map((t: { name: string }) => t.name);
  }

  async getTableMetadata(namespace: string, table: string): Promise<unknown> {
    const response = await fetch(
      `${this.config.baseUrl}/v1/namespaces/${namespace}/tables/${table}`
    );
    return response.json();
  }
}

async function main() {
  const client = new IcebergRestClient({
    baseUrl: "http://localhost:8181",
    warehouse: "demo",
  });

  try {
    console.log("Namespaces:", await client.listNamespaces());
    console.log("Tables:", await client.listTables("db"));
    const metadata = await client.getTableMetadata("db", "orders");
    console.log("Metadata:", JSON.stringify(metadata, null, 2));
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
```

---

### 4-8: Iceberg V3の新機能

> **Note**: Iceberg V3 spec（バージョン 1.8.0〜1.10.0）で導入された主要な新機能です。

#### Deletion Vectors（削除ベクトル）

V3ではバイナリ形式の削除ベクトルが導入され、行レベル削除が大幅に効率化されました：

```
従来（V2）:
├── データファイル + 削除ファイルをマージして読み取り
└── 削除操作のたびにファイル書き換えが発生

V3（Deletion Vectors）:
├── ビットマップ形式で削除情報を保持
├── 読み取り時に適用（merge-on-read）
└── 書き込みコストを大幅に削減
```

```sql
-- Deletion Vectorsの有効化
ALTER TABLE demo.db.orders
SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');

-- 効率的な行削除（ファイル書き換えなし）
DELETE FROM demo.db.orders WHERE status = 'cancelled';

-- 読み取り時に削除情報が適用される
SELECT * FROM demo.db.orders;
```

#### VARIANT型（セミ構造化データ）

JSONライクな柔軟なデータを型安全に扱えます：

```sql
-- VARIANT型カラムを持つテーブル
CREATE TABLE demo.db.events (
    event_id BIGINT,
    event_time TIMESTAMP,
    payload VARIANT
) USING iceberg;

-- JSONデータの挿入
INSERT INTO demo.db.events VALUES
    (1, current_timestamp(), PARSE_JSON('{"action": "click", "target": "button1", "metadata": {"version": 2}}')),
    (2, current_timestamp(), PARSE_JSON('{"action": "purchase", "item_id": 12345, "price": 99.99}'));

-- VARIANT内のフィールドにアクセス（ドット記法）
SELECT
    event_id,
    payload:action AS action,
    payload:metadata:version AS version
FROM demo.db.events;

-- JSON関数との組み合わせ
SELECT
    event_id,
    JSON_VALUE(payload, '$.action') AS action
FROM demo.db.events
WHERE JSON_VALUE(payload, '$.action') = 'purchase';
```

#### デフォルトカラム値

カラム追加時にデフォルト値を指定可能（メタデータのみの変更、データファイル書き換え不要）：

```sql
-- デフォルト値付きでカラム追加（瞬時に完了）
ALTER TABLE demo.db.orders ADD COLUMN version INT DEFAULT 1;
ALTER TABLE demo.db.orders ADD COLUMN region STRING DEFAULT 'unknown';

-- 既存データは読み取り時にデフォルト値が適用される
SELECT order_id, version, region FROM demo.db.orders LIMIT 5;
-- 結果: version=1, region='unknown' が返される（ファイルには値がない）
```

#### Nanosecond Precision Timestamps

イベント処理やテレメトリーワークロード向けの高精度タイムスタンプ：

```sql
CREATE TABLE demo.db.telemetry (
    sensor_id STRING,
    event_time TIMESTAMP_NS,  -- ナノ秒精度
    value DOUBLE
) USING iceberg;
```

#### Geospatial Types（地理空間型）

位置情報分析やマッピングユースケース向け：

```sql
CREATE TABLE demo.db.locations (
    id BIGINT,
    name STRING,
    location GEOMETRY,        -- 幾何データ
    coverage GEOGRAPHY        -- 地理データ（球面座標系）
) USING iceberg;
```

---

## 補足資料

### 用語集（Webエンジニア向け対応表）

| データエンジニアリング用語 | Webエンジニアの類似概念 |
|---------------------------|------------------------|
| ETL | バッチ処理、データ同期ジョブ |
| パーティション | DBのパーティション、シャーディング |
| カタログ | スキーマレジストリ、メタデータDB |
| スナップショット | Gitのコミット |
| タイムトラベル | Gitのチェックアウト |
| Compaction | DBのVACUUM |
| データレイク | オブジェクトストレージ上のファイル群 |
| DuckDB | SQLiteのOLAP版 |
| Deletion Vectors | 論理削除フラグ（V3新機能） |
| VARIANT型 | JSONカラム、NoSQLドキュメント |
| Merge-on-Read | 遅延評価、読み取り時結合 |

---

### package.json

```json
{
  "name": "data-engineering-learning",
  "version": "1.0.0",
  "scripts": {
    "week1:oltp-olap": "ts-node src/week1/oltp-vs-olap.ts",
    "week1:csv-parquet": "ts-node src/week1/csv-vs-parquet.ts",
    "week2:metadata": "ts-node src/week2/parquet-metadata.ts",
    "week2:partition": "ts-node src/week2/partitioning.ts",
    "week2:limitations": "ts-node src/week2/hive-limitations.ts",
    "week3:basics": "ts-node src/week3/duckdb-basics.ts",
    "week3:explain": "ts-node src/week3/explain-plan.ts",
    "week3:benchmark": "ts-node src/week3/performance-comparison.ts"
  },
  "dependencies": {
    "@duckdb/node-api": "^1.4.0",
    "pg": "^8.16.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "@types/pg": "^8.10.0",
    "ts-node": "^10.9.0",
    "typescript": "^5.9.0"
  }
}
```

> **Note**: 旧 `duckdb` パッケージは非推奨です。新しい `@duckdb/node-api`（DuckDB Node Neo）を使用してください。
> Node Neo は Promise ベースで TypeScript ネイティブ対応しています。

---

## 参考リソース

### Apache Iceberg
- [Apache Iceberg公式ドキュメント](https://iceberg.apache.org/docs/latest/)
- [Iceberg V3の新機能](https://opensource.googleblog.com/2025/08/whats-new-in-iceberg-v3.html)
- [PyIceberg](https://py.iceberg.apache.org/) - PythonからIcebergを操作

### DuckDB
- [DuckDB公式ドキュメント](https://duckdb.org/docs/)
- [DuckDB Node Neo](https://duckdb.org/docs/stable/clients/node_neo/overview) - 新しいNode.js API
- [@duckdb/node-api (npm)](https://www.npmjs.com/package/@duckdb/node-api)

### Apache Spark
- [Apache Spark 4.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Spark and Iceberg Quickstart](https://iceberg.apache.org/spark-quickstart/)

### Docker環境
- [docker-spark-iceberg](https://github.com/databricks/docker-spark-iceberg)
- [Apache Spark Docker Images](https://hub.docker.com/r/apache/spark)