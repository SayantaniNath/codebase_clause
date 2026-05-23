# Data Engineer Interview Prep Guide

Covers all three rounds from the screenshot. Each answer follows a structured
format so you can practise delivering it in 2–3 minutes.

---

## Round 1 — Technical

### Q1. How would you design a real-time data pipeline to process millions of financial transactions for fraud detection?

**Architecture (what to draw on a whiteboard):**

```
Transactions --> Kafka (partitioned by account_id)
                    |
           Spark Structured Streaming / Flink
                    |
          +---------+---------+
          |                   |
     Rule Engine         ML Scorer
  (velocity, geo,     (XGBoost / LightGBM)
   device, amount)          |
          |                   |
          +---------+---------+
                    |
              Risk Score
                    |
         +----------+----------+
         |                     |
    HIGH_RISK              LOW_RISK
    Kafka alert topic      Cassandra write
         |
    Alert service --> human review queue
```

**Key design decisions to mention:**

1. **Ingestion** — Kafka partitioned by `account_id` ensures all transactions for one account land on the same partition → consistent ordering for velocity checks.
2. **Feature store** — Redis holds rolling counters (transactions in last 1 min / 5 min / 1 hour per account). Stream processor increments counters atomically using `INCR` + TTL.
3. **Dual-layer detection:**
   - *Rule engine* (deterministic, <1 ms): velocity limits, geo-distance impossibility, known fraud device fingerprints.
   - *ML model* (probabilistic, <10 ms): trained on labelled fraud cases, outputs probability score.
4. **Decision** — combine rule + ML scores, apply threshold → BLOCK / REVIEW / ALLOW.
5. **Feedback loop** — analyst decisions flow back to a labelled training dataset; model retrained weekly.
6. **Latency target** — <100 ms end-to-end (Kafka offset to decision) for real-time card authorisation.

**How this project demonstrates it:**
- `src/fraud_detector.py` implements the rule engine with 7 behavioural signals.
- Kafka consumer (`src/consumer.py`) runs scoring inline before writing to Redis.
- GraphQL API exposes `getHighRiskUsers` and `getSuspiciousUsers` for the review queue.

---

### Q2. How do you guarantee data consistency between a production OLTP database and an analytical warehouse?

**Strategy:**

1. **CDC with Debezium + Kafka** — Debezium reads the PostgreSQL WAL (write-ahead log) and emits every INSERT/UPDATE/DELETE as a Kafka event. Nothing is polled; every row change is captured.
2. **Exactly-once semantics** — Kafka transactions + idempotent consumer writes (upsert with `ON CONFLICT DO UPDATE` in the warehouse) prevent duplicates.
3. **Schema registry** — Avro schemas are registered in Confluent Schema Registry; producers and consumers enforce compatibility so schema drift is caught before data breaks.
4. **Reconciliation jobs** — nightly Spark job compares row counts and checksums between PostgreSQL and warehouse tables; alerts on discrepancy > 0.01%.
5. **SLA monitoring** — track replication lag (Kafka consumer offset lag); alert if lag > 5 minutes.

**Common failure modes and mitigations:**

| Failure | Mitigation |
|---|---|
| Network partition | Kafka retention set to 7 days; consumer replays from last committed offset |
| Schema change | Schema Registry enforces backward compatibility; breaking changes require version bump |
| Warehouse write failure | Dead-letter queue; replay job re-processes failed records |
| Clock skew | Use source database timestamps, not consumer timestamps |

---

### Q3. You've discovered duplicate records in a critical production dataset. Walk through your plan.

**Immediate (first 30 minutes):**

1. Quantify scope — how many duplicates, which tables, which date range, what's the business key?
   ```sql
   SELECT business_key, COUNT(*) AS cnt
   FROM orders
   GROUP BY business_key HAVING COUNT(*) > 1;
   ```
2. Do NOT delete yet — create an audit snapshot:
   ```sql
   CREATE TABLE orders_duplicate_audit AS
   SELECT * FROM orders WHERE business_key IN (SELECT business_key FROM ...);
   ```
3. Assess downstream impact — which dashboards, reports, or services consume this table?
4. Communicate status to stakeholders.

**Deduplication (careful):**
```sql
-- Keep the most recent record per business key
DELETE FROM orders
WHERE id NOT IN (
    SELECT DISTINCT ON (business_key) id
    FROM orders
    ORDER BY business_key, created_at DESC
);
```

**Long-term prevention:**

| Layer | Fix |
|---|---|
| Database | `UNIQUE` constraint on `business_key` — database rejects duplicates at insert |
| Pipeline | Idempotent upsert: `INSERT ... ON CONFLICT (business_key) DO UPDATE` |
| Kafka consumer | Maintain a "seen" set in Redis; skip messages with already-processed IDs |
| Data quality | Great Expectations / dbt tests run after every pipeline load; fail loudly |

---

### Q4. A producer team is pushing a schema change that will break your downstream pipeline. How do you manage this?

**Prevention (before the break):**

1. **Schema Registry** — every schema change must pass a compatibility check:
   - *Backward compatible*: new optional fields only → consumers can still read old messages.
   - *Full compatibility*: both old and new consumers can read both old and new messages.
2. **Contract testing** — consumer team publishes a "consumer contract" (Pact tests); producer CI runs it before merging.

**If you get surprise notice of an upcoming breaking change:**

1. Negotiate a transition window (ideally 2–4 weeks).
2. **Dual-publish** — producer writes to both old topic (old schema) and new topic (new schema) during the transition.
3. Migrate consumer to read from new topic.
4. Deprecate old topic once all consumers are migrated.

**Defensive coding in the consumer:**
```python
# Never do: record["new_field"]  — breaks if field absent
# Always do:
value = record.get("new_field", default_value)
```

**If you inherit a broken pipeline today:**

1. Stop the consumer to prevent bad data from propagating.
2. Determine the offset where the schema changed.
3. Write a migration script to transform old-schema messages to new schema.
4. Replay from that offset with the migration applied.

---

### Q5. How do you systematically debug a Spark job running significantly slower in production than in testing?

**Step-by-step diagnosis:**

1. **Open Spark UI** (`:4040`) → look at the stage timeline. Find which stage takes longest.
2. **Data skew check** — in the slow stage, look at task durations. If one task takes 10× longer than others, you have skew.
   ```python
   # Fix: salting
   df = df.withColumn("salt", (rand() * 10).cast("int"))
   df = df.withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))
   ```
3. **Shuffle read/write** — large shuffle = expensive. Look for unexpected `Exchange` nodes in the query plan. Fix: broadcast the smaller side.
   ```python
   from pyspark.sql.functions import broadcast
   result = large_df.join(broadcast(small_df), "id")
   ```
4. **GC pressure** — Spark UI shows GC time per executor. >10% GC time = memory pressure. Fix: increase executor memory or switch to off-heap storage.
5. **Input data volume** — production may have more data than test. Check number of input files and partition sizes. Target 128–256 MB per partition.
6. **External I/O** — slow reads from S3/HDFS, database connections being opened per-task, etc.
7. **Serialization** — use Kryo instead of Java serialization:
   ```python
   spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   ```

**Quick wins checklist:**
- [ ] Predicate pushdown enabled (filter before join)
- [ ] Partition by join key before wide transformation
- [ ] Adaptive Query Execution enabled (`spark.sql.adaptive.enabled=true`)
- [ ] Statistics up to date (`ANALYZE TABLE`)

---

## Round 2 — Technical

### Q1. How do you design a system to secure PII within a data lake while allowing authorized analytical access?

**Defence-in-depth approach:**

```
Raw Zone (encrypted)
  PII fields encrypted with AES-256
  KMS manages encryption keys
       |
       v
Refined Zone (tokenised)
  Name/email/phone replaced with tokens
  Token↔PII mapping in secure vault (HashiCorp Vault)
       |
       v
Analytical Zone (masked / anonymised)
  Analysts see: name → "J*** D***", email → "j***@g***.com"
  Aggregations allowed; individual PII not exposed
```

**Access control layers:**

| Layer | Tool | What it enforces |
|---|---|---|
| Storage | S3 bucket policies / HDFS ACLs | Who can read raw files |
| Catalogue | AWS Lake Formation / Apache Ranger | Column-level access per role |
| Query engine | Row-level security in Trino/Spark | Filter rows by user's data jurisdiction |
| Audit | CloudTrail / Ranger audit log | Who queried which PII column, when |

**GDPR compliance hooks:**
- Data subject deletion: tokenisation means you delete the token mapping → all derived data automatically becomes anonymous.
- Purpose limitation: tag each column with its purpose; enforce via policy that analytics jobs can only read columns matching their declared purpose.

**This project's approach** (`src/fraud_detector.py`):
- PII fields (email, phone, name) are used for scoring but never logged in fraud signals.
- Fraud results stored in Redis contain only `user_id` and behavioral scores — no raw PII.

---

### Q2. Your real-time dashboard is lagging because of Kafka pipeline delays. What's your troubleshooting methodology?

**Systematic diagnosis:**

1. **Measure consumer lag:**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --describe --group dashboard-consumer-group
   ```
   Shows lag per partition. Pinpoint which partition is behind.

2. **Is the producer slow or the consumer slow?**
   - Check *producer throughput* (messages/sec in Kafka broker metrics).
   - Check *consumer throughput* (messages consumed/sec).
   - If producer > consumer → consumer is the bottleneck.

3. **Consumer bottleneck causes:**
   - Too few consumer instances (max parallelism = number of partitions).
   - Message processing logic is slow (DB write, external API call per message).
   - Consumer re-balancing loop (check for frequent re-joins in consumer logs).

4. **Broker-side issues:**
   - High disk I/O → messages not replicated fast enough.
   - Leader election happening (check controller logs).
   - Retention limit hit → old messages deleted before consumed.

5. **Fixes:**
   - Add consumer instances (scale horizontally up to partition count).
   - Increase partition count (requires topic recreation or reassignment).
   - Batch writes instead of per-message writes.
   - Move heavy processing off the hot path (async enrichment).

---

### Q3. What is your emergency action plan if a critical data pipeline fails during peak business hours?

**Incident response playbook:**

```
DETECT → TRIAGE → CONTAIN → DIAGNOSE → FIX → VERIFY → COMMUNICATE → POST-MORTEM
```

| Phase | Actions | Time target |
|---|---|---|
| Detect | PagerDuty alert fires; on-call engineer acknowledges | <5 min |
| Triage | Is data lost, delayed, or corrupted? What's the blast radius? | <10 min |
| Contain | Pause downstream consumers to prevent bad data propagation | <15 min |
| Diagnose | Check logs, metrics, recent deployments, infra events | <30 min |
| Fix | Rollback deployment OR apply hotfix | <60 min |
| Verify | Confirm pipeline healthy; run reconciliation check | +15 min |
| Communicate | Stakeholder update every 15–30 min during incident | Ongoing |
| Post-mortem | Blameless root cause analysis within 48 hours | <48 h |

**Key principle — preserve data first:**
- Ensure Kafka retention is set to ≥24 hours so no messages are lost during the outage.
- Do NOT truncate or rollback data until root cause is confirmed.

**Rollback checklist:**
1. Identify last known good deployment.
2. Redeploy previous version.
3. Replay Kafka from offset where failure began.
4. Validate row counts match expected.

---

### Q4. How do you strategically manage data partitioning in a petabyte-scale dataset to avoid data skew?

**Partition key selection:**

| Key type | Good for | Risk |
|---|---|---|
| Hash of user_id | Even distribution across partitions | No time-based pruning |
| Date (year/month/day) | Time-range queries | Hot partition on "today" |
| Composite (date + region) | Both time and geography queries | More complex queries needed |

**Detecting skew:**
```python
# In Spark — check partition sizes
df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
# If one value is 100× the median, you have skew
```

**Fixing skew:**

1. **Salting** (for joins/aggregations with hot keys):
   ```python
   df = df.withColumn("salt", (rand() * 10).cast("int"))
   df_joined = df.join(df2, [df.key == df2.key, df.salt == df2.salt])
   ```

2. **Repartition** before write:
   ```python
   df.repartition(200, "country", "date").write.parquet(path)
   ```

3. **Z-ORDER (Delta Lake)** for multi-column pruning:
   ```sql
   OPTIMIZE events ZORDER BY (user_id, event_date)
   ```

4. **File sizing** — target 256 MB–1 GB per Parquet file; avoid too many small files (metadata overhead) and too few large files (parallelism bottleneck).

---

### Q5. How would you join two massive datasets that are too large to fit into memory in a distributed environment?

**Technique selection:**

| Situation | Technique |
|---|---|
| One side fits in memory (<2 GB) | **Broadcast hash join** — ship small table to every executor |
| Both sides large, pre-bucketed on join key | **Bucket join** — no shuffle needed, each bucket joined locally |
| Both sides large, arbitrary | **Sort-merge join** — partition both by join key, sort, merge |
| Only need approximate results | **Bloom filter pre-filtering** — eliminate non-matching rows before join |

**Sort-merge join in practice:**
```python
# Pre-partition both datasets by join key before writing to storage
# Then Spark reads matching partitions together — no shuffle at query time
df_orders.repartition(200, "user_id").write.partitionBy("user_id").parquet("orders/")
df_users.repartition(200, "user_id").write.partitionBy("user_id").parquet("users/")

# At query time — Spark detects matching partitioning → no shuffle
spark.read.parquet("orders/").join(spark.read.parquet("users/"), "user_id")
```

**Additional tactics:**
- **Incremental join** — if only new rows need joining, join only the delta.
- **Columnar pruning** — select only columns needed before the join to reduce shuffle volume.
- **AQE (Adaptive Query Execution)** — let Spark dynamically switch join strategies at runtime.

---

### Q6. A daily batch job that normally takes 2 hours is now taking 6 hours. How do you identify the root cause?

**Structured investigation (start broad, narrow down):**

1. **Input data volume** — has the data grown 3×?
   ```bash
   # Compare today's input file size vs last week
   hdfs dfs -du -s /data/input/$(date +%Y-%m-%d)
   ```

2. **Resource contention** — is the cluster busier than normal? Check YARN resource manager or Kubernetes pod scheduling delays.

3. **Spark UI comparison** — open the slow run vs a fast historical run side-by-side:
   - Which stage is slow?
   - Are there more tasks than before?
   - Is GC time elevated?

4. **Shuffle explosion** — an upstream data change (e.g., new category added to a GROUP BY column) can multiply shuffle size.

5. **External dependency degradation** — DB connection pool exhausted? S3 throttling? Network latency spike?

6. **Data quality regression** — unexpected NULLs or malformed records triggering excessive exception handling / retries.

7. **Code path change** — was there a deployment between the last fast run and today?

**Root cause template for communicating findings:**
> "The job slowed because [X]. This was triggered by [Y change]. Impact: [Z hours extra]. Fix: [action taken]. Prevention: [monitoring/test added]."

---

## Round 3 — Managerial / Behavioural

> Use the **STAR method** for all behavioural answers:
> **S**ituation → **T**ask → **A**ction → **R**esult

---

### Q1. Tell me about a time you handled a production issue under pressure.

**Example answer structure:**

- **Situation**: Our main data pipeline feeding the executive dashboard went down at 9 AM on a Monday — the first business day of quarter-end reporting.
- **Task**: I was the on-call engineer. I needed to restore data within the SLA (2 hours) and communicate status clearly to leadership.
- **Action**:
  1. Pulled recent deployment log — found a Kafka broker had been restarted for maintenance, triggering a consumer group re-balance storm.
  2. Paused all consumers to stop the re-balance loop.
  3. Manually reset consumer offsets to the last committed checkpoint.
  4. Restarted consumers one at a time to stagger re-joining.
  5. Posted status updates in the incident Slack channel every 15 minutes.
- **Result**: Pipeline restored in 45 minutes. No data was lost (Kafka retention was 24 hours). Post-mortem added a canary consumer restart procedure and a re-balance rate alert to prevent recurrence.

**Key traits to convey**: calm under pressure, methodical diagnosis, proactive communication, focus on preventing recurrence.

---

### Q2. How do you explain technical solutions to non-technical clients?

**Framework:**

1. **Lead with the business outcome**, not the technology.
   - Not: "We implemented CDC with Debezium and Kafka."
   - Yes: "Your reports will now update within 5 minutes instead of overnight."

2. **Use analogies** — map unfamiliar concepts to everyday objects.
   - Kafka = "a post office that keeps every letter for 7 days, so nothing is lost even if the recipient is temporarily unavailable."
   - Redis cache = "a whiteboard next to your desk — faster to look at than going to the filing cabinet every time."

3. **Quantify the benefit** — clients respond to numbers.
   - "This reduces your report refresh time from 8 hours to 10 minutes."
   - "This saves approximately £X/month in query compute costs."

4. **Check for understanding** — ask, don't assume.
   - "Does this make sense so far? Is there a part I should explain differently?"

5. **Avoid jargon** unless the client has shown they're comfortable with it.

---

### Q3. A client has unrealistic delivery timeline expectations. How do you handle it?

**Steps:**

1. **Acknowledge their urgency first** — understand why the deadline matters to them (regulatory, competitive, contractual?).

2. **Show your work** — present a breakdown of tasks, dependencies, and risks. Numbers are harder to argue with than feelings.
   ```
   Feature A: 3 days (blocks B and C)
   Feature B: 5 days
   Feature C: 2 days + 1 day QA
   Total: 11 days minimum — current ask is 5 days
   ```

3. **Offer options, not a flat no:**
   - *Option 1*: Phased delivery — deliver MVP (core features) by their date; full scope follows 3 weeks later.
   - *Option 2*: Scope reduction — which features are truly required for the deadline?
   - *Option 3*: Additional resources — can we add a second engineer to parallelize work?

4. **Document the agreed compromise** in writing (email recap after the call).

5. **Be honest about risk** — if rushing creates technical debt or quality issues, say so clearly so the client owns that trade-off.

---

### Q4. Describe a situation where you worked with multiple teams having conflicting priorities. How did you manage deadlines?

**Example answer structure:**

- **Situation**: The data platform team, the analytics team, and the product team all needed deliverables from me in the same two-week sprint.
- **Task**: Prioritise work, manage expectations, and deliver without burning out or dropping quality.
- **Action**:
  1. Listed all requests with their deadlines and business impact.
  2. Identified the critical path — the data platform migration blocked the analytics team's work, so it had to go first.
  3. Escalated the conflict to our engineering manager with a clear dependency map. Leadership arbitrated the final priority order.
  4. Communicated the agreed sequence to all three teams so each knew when to expect their deliverable and why.
  5. Used a shared Jira board so all stakeholders had visibility without needing to ask me for status.
- **Result**: All three deliverables shipped within the sprint — the migration on day 4, analytics on day 8, product feature on day 12. No surprises for any team.

**Key traits to convey**: structured thinking, escalation when appropriate (not trying to be a hero), transparency, and keeping everyone informed.

---

## How to Practise These Answers

1. **Record yourself** — speak each answer aloud and time it. Target 2–3 minutes per answer.
2. **Draw the architecture** — for Q1, Q4 (Round 1) and Q1–Q5 (Round 2), practice sketching on a whiteboard. Interviewers value visual communication.
3. **Tie answers to this codebase** — for every technical question, be ready to say "I implemented something similar in [file]" and explain your design choices.
4. **Prepare your own STAR stories** — replace the example stories in Round 3 with your actual experiences. Authenticity matters more than polish.
5. **Mock interviews** — do at least one mock interview per round with a peer, focusing on questions you find hardest.

---

## Quick Reference: Fraud Detection in This Project

| Component | File | What it does |
|---|---|---|
| Rule engine | `src/fraud_detector.py` | Scores each user record; returns risk level + triggered signals |
| Consumer integration | `src/consumer.py` | Calls `score_record()` on every Kafka message; stores fraud result in Redis |
| GraphQL API | `src/api.py` | `getHighRiskUsers`, `getSuspiciousUsers`, `getFraudResult`, `getAllFraudAlerts` |
| Risk levels | `fraud_detector.py` | CLEAN (0–30), SUSPICIOUS (31–60), HIGH_RISK (61+) |
| Rules | `fraud_detector.py` | 7 rules: unverified spend, no-2FA enterprise, zero-login purchase, low-engagement spend, risk tag, inactive purchase, large purchase |
