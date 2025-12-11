# データエンジニアリング入門 → Apache Iceberg 習得カリキュラム

Webエンジニア向けに、データエンジニアリングの基礎からApache Icebergの実践までを段階的に学ぶカリキュラムです。

**使用言語**: TypeScript（Node.js）

## 前提環境

```bash
# Node.js 20以上推奨
node -v

# プロジェクト初期化
mkdir data-engineering-learning
cd data-engineering-learning
npm init -y
npm install typescript ts-node @types/node -D
npx tsc --init

# 必要なパッケージ（Week毎に追加）
npm install duckdb
npm install pg @types/pg           # PostgreSQL
```

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
import * as duckdb from "duckdb";
import * as fs from "fs";

async function main() {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  const dataDir = "./data";
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // 100万行のデータを生成してCSVとParquetで保存
  console.log("Generating 1,000,000 rows...");

  await runQuery(conn, `
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
  await runQuery(conn, `COPY events TO '${dataDir}/data.csv' (HEADER, DELIMITER ',');`);

  // Parquetで保存
  console.log("Exporting to Parquet...");
  await runQuery(conn, `COPY events TO '${dataDir}/data.parquet' (FORMAT PARQUET);`);

  // ファイルサイズ比較
  const csvSize = fs.statSync(`${dataDir}/data.csv`).size;
  const parquetSize = fs.statSync(`${dataDir}/data.parquet`).size;

  console.log("\n=== File Size Comparison ===");
  console.log(`CSV:     ${(csvSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Parquet: ${(parquetSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Ratio:   ${(csvSize / parquetSize).toFixed(2)}x smaller`);

  conn.close();
  db.close();
}

function runQuery(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.run(sql, (err) => (err ? reject(err) : resolve()));
  });
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
import * as duckdb from "duckdb";

async function main() {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  console.log("=== Parquet Metadata ===\n");

  // スキーマ確認
  console.log("Schema:");
  await query(conn, `DESCRIBE SELECT * FROM './data/data.parquet'`);

  // Parquetメタデータの詳細
  console.log("\nFile Metadata:");
  await query(conn, `SELECT * FROM parquet_metadata('./data/data.parquet')`);

  // Row Groupごとの統計情報
  console.log("\nColumn Statistics per Row Group:");
  await query(conn, `
    SELECT 
      row_group_id,
      column_id, 
      path_in_schema as column_name,
      num_values,
      stats_min,
      stats_max
    FROM parquet_metadata('./data/data.parquet')
  `);

  conn.close();
  db.close();
}

function query(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, result) => {
      if (err) reject(err);
      console.table(result);
      resolve();
    });
  });
}

main().catch(console.error);
```

**確認ポイント**: 統計情報があることで、クエリ時に不要なRow Groupをスキップできる

---

### 2-2: パーティショニングの基礎

#### 課題2-2: パーティション分割の実践

```typescript
// src/week2/partitioning.ts
import * as duckdb from "duckdb";
import * as fs from "fs";
import * as path from "path";

async function main() {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  const partitionedDir = "./data/partitioned_data";
  if (fs.existsSync(partitionedDir)) {
    fs.rmSync(partitionedDir, { recursive: true });
  }

  // 1年分のデータを作成
  console.log("Creating 1 year of event data...");

  await runQuery(conn, `
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

  await runQuery(conn, `
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
  await query(conn, `SELECT COUNT(*), AVG(amount) FROM '${partitionedDir}/**/*.parquet'`);
  console.timeEnd("full-scan");

  console.log("\nPartition pruning (January only):");
  console.time("partition-pruning");
  await query(conn, `SELECT COUNT(*), AVG(amount) FROM '${partitionedDir}/year=2024/month=1/*.parquet'`);
  console.timeEnd("partition-pruning");

  conn.close();
  db.close();
}

function listDir(dir: string, indent = "") {
  const items = fs.readdirSync(dir);
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      console.log(`${indent}📁 ${item}/`);
      listDir(fullPath, indent + "  ");
    } else {
      const sizeKB = (stat.size / 1024).toFixed(1);
      console.log(`${indent}📄 ${item} (${sizeKB} KB)`);
    }
  }
}

function runQuery(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.run(sql, (err) => (err ? reject(err) : resolve()));
  });
}

function query(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, result) => {
      if (err) reject(err);
      console.table(result);
      resolve();
    });
  });
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
import * as duckdb from "duckdb";

async function main() {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  console.log("=== DuckDB Basics ===\n");

  // Parquetファイルを直接クエリ（テーブル作成不要）
  console.log("1. Query Parquet directly:");
  await query(conn, `SELECT * FROM './data/data.parquet' LIMIT 5`);

  // SQLでの集計
  console.log("\n2. Aggregation:");
  await query(conn, `
    SELECT 
      event_type,
      COUNT(*) as count,
      ROUND(AVG(amount), 2) as avg_amount,
      ROUND(SUM(amount), 2) as total_amount
    FROM './data/data.parquet'
    GROUP BY event_type
    ORDER BY count DESC
  `);

  // 時系列分析
  console.log("\n3. Time series analysis:");
  await query(conn, `
    SELECT 
      DATE_TRUNC('month', timestamp) as month,
      COUNT(*) as events,
      ROUND(SUM(amount), 2) as revenue
    FROM './data/data.parquet'
    GROUP BY 1
    ORDER BY 1
    LIMIT 6
  `);

  // Window関数
  console.log("\n4. Window functions (running total):");
  await query(conn, `
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

  // 複数ファイルの結合
  console.log("\n5. Query partitioned data with glob:");
  await query(conn, `
    SELECT 
      year, month, COUNT(*) as events
    FROM './data/partitioned_data/**/*.parquet'
    GROUP BY year, month
    ORDER BY year, month
    LIMIT 6
  `);

  conn.close();
  db.close();
}

function query(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, result) => {
      if (err) reject(err);
      console.table(result);
      resolve();
    });
  });
}

main().catch(console.error);
```

---

### 3-3: 実行計画の読み方

#### 課題3-3: EXPLAINで処理を理解

```typescript
// src/week3/explain-plan.ts
import * as duckdb from "duckdb";

async function main() {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  console.log("=== Query Execution Plans ===\n");

  // シンプルなクエリ
  console.log("1. Simple aggregation plan:");
  await explain(conn, `
    SELECT event_type, SUM(amount)
    FROM './data/data.parquet'
    WHERE amount > 100
    GROUP BY event_type
  `);

  // フィルタ付きクエリ
  console.log("\n2. With predicate pushdown:");
  await explain(conn, `
    SELECT COUNT(*)
    FROM './data/data.parquet'
    WHERE user_id = 1234
  `);

  conn.close();
  db.close();
}

function explain(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.all(`EXPLAIN ANALYZE ${sql}`, (err, result) => {
      if (err) reject(err);
      if (result && result.length > 0) {
        const output = (result[0] as Record<string, unknown>)["explain_value"] 
          || (result[0] as Record<string, unknown>)["EXPLAIN ANALYZE"]
          || JSON.stringify(result[0], null, 2);
        console.log(output);
      }
      resolve();
    });
  });
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
import * as duckdb from "duckdb";
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

  const duckDb = new duckdb.Database(":memory:");
  const duckConn = duckDb.connect();

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
  await runDuckQuery(duckConn, `
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
  await runDuckQuery(duckConn, "SELECT * FROM events WHERE id = 500000");
  console.timeEnd("DuckDB");
  console.log("→ PostgreSQL wins (indexed lookup)\n");

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
  await runDuckQuery(duckConn, `
    SELECT event_type, COUNT(*), AVG(amount) 
    FROM events 
    GROUP BY event_type
  `);
  console.timeEnd("DuckDB");
  console.log("→ DuckDB wins (columnar scan)\n");

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
  await runDuckQuery(duckConn, complexQuery);
  console.timeEnd("DuckDB");
  console.log("→ DuckDB wins significantly\n");

  await pgClient.end();
  duckConn.close();
  duckDb.close();
}

function runDuckQuery(conn: duckdb.Connection, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err) => (err ? reject(err) : resolve()));
  });
}

main().catch(console.error);
```

---

## Week 4-5: Apache Iceberg実践

> **Note**: Icebergの操作には現時点でSpark/Trino/Flinkなどが必要です。
> TypeScriptからはREST Catalog APIを通じてメタデータ操作が可能ですが、
> データ操作の実践はSpark SQLを使用します。

### 4-1: Iceberg環境構築

```yaml
# docker-compose.yml
version: '3.8'
services:
  spark-iceberg:
    image: tabulario/spark-iceberg:3.5.1_1.5.2
    container_name: spark-iceberg
    ports:
      - "8888:8888"  # Jupyter
      - "8080:8080"  # Spark UI
      - "10000:10000"
      - "10001:10001"
    volumes:
      - ./warehouse:/home/iceberg/warehouse
      - ./notebooks:/home/iceberg/notebooks
```

```bash
docker-compose up -d
# Jupyter Notebookが http://localhost:8888 で起動
```

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
    "duckdb": "^1.0.0",
    "pg": "^8.11.0"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "@types/pg": "^8.10.0",
    "ts-node": "^10.9.0",
    "typescript": "^5.0.0"
  }
}
```

---

## 参考リソース

- [Apache Iceberg公式ドキュメント](https://iceberg.apache.org/docs/latest/)
- [DuckDB公式ドキュメント](https://duckdb.org/docs/)
- [Tabular社チュートリアル](https://tabular.io/blog/)
- [docker-spark-iceberg](https://github.com/tabular-io/docker-spark-iceberg)