# Setup — Lokale Reproduzierbarkeit

Schritt-für-Schritt-Anleitung zur vollständigen Einrichtung des Data Warehouse auf einer lokalen SQL Server Instanz.

---

## Voraussetzungen

- MS SQL Server (Developer Edition oder höher)
- SSMS 19+
- Olist-Datensatz lokal verfügbar → [Kaggle Download](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Ausführungsreihenfolge

Alle Skripte werden in SSMS ausgeführt. Die Reihenfolge ist zwingend — spätere Skripte referenzieren Objekte aus früheren.

### 1. Datenbank anlegen

In SSMS eine neue Datenbank `OlistDWH` erstellen (per UI oder manuell):

```sql
CREATE DATABASE OlistDWH;
```

### 2. Schemas anlegen

```
sql/create_schemas.sql
```

Legt die Schemas `raw`, `cleansed`, `mart`, `audit`, `orchestration` an.

### 3. Audit-Tabellen

```
sql/audit/schema/create_audit_tables.sql
```

Muss vor den anderen Schemas ausgeführt werden, da `audit.job_log` von den FK-Constraints in `orchestration.pipeline_config` referenziert wird.

### 4. Orchestrierung — Tabellen & Trigger

```
sql/orchestration/schema/create_orchestration_tables.sql
sql/orchestration/schema/create_orchestration_triggers.sql
```

`create_orchestration_triggers.sql` legt den AFTER UPDATE Trigger an, der `modified_ts` in `pipeline_config` automatisch aktualisiert.

### 5. Raw-Tabellen

```
sql/raw/schema/create_raw_tables.sql
```

Enthält alle `CREATE TABLE` Definitionen sowie die zugehörigen Non-Clustered Indexes auf `batch_id`.

### 6. Cleansed-Tabellen

```
sql/cleansed/schema/create_cleansed_tables.sql
```

### 7. Mart-Tabellen

```
sql/mart/schema/create_mart_tables.sql
```

Aktuell Platzhalter — wird mit der Mart-Implementierung befüllt.

### 8. Stored Procedures deployen

Alle Dateien sind idempotent (`CREATE OR ALTER`) und können in beliebiger Reihenfolge innerhalb der Gruppe ausgeführt werden.

**RAW:**
```
sql/raw/procedures/raw_sp_load_customers.sql
sql/raw/procedures/raw_sp_load_orders.sql
sql/raw/procedures/raw_sp_load_order_items.sql
sql/raw/procedures/raw_sp_load_order_payments.sql
```

**CLEANSED:**
```
sql/cleansed/procedures/cleansed_sp_load_customers.sql
sql/cleansed/procedures/cleansed_sp_load_orders.sql
sql/cleansed/procedures/cleansed_sp_load_order_items.sql
```

**Orchestrierung:**
```
sql/orchestration/procedures/orchestration_sp_run_layer.sql
sql/orchestration/procedures/orchestration_sp_run_full_load.sql
```

### 9. Pipeline-Konfiguration befüllen

```
sql/orchestration/config/dev_pipeline_config.sql
```

**Vor der Ausführung:** `@DatasetRoot` in Zeile 11 auf den lokalen Ordner mit den Olist-CSV-Dateien setzen (abschließender Backslash erforderlich):

```sql
DECLARE @DatasetRoot NVARCHAR(500) = 'C:\Dein\Pfad\olist_data\';
```

Das Skript ist idempotent — bereits vorhandene Einträge werden übersprungen.

### 10. SQL Server Agent Job registrieren (optional)

```
sql/orchestration/jobs/agent_job_full_load.sql
```

Registriert einen Agent Job `OlistDWH_FullLoad`, der `orchestration.sp_run_full_load` aufruft. Nur relevant wenn der Job über den SQL Server Agent geplant werden soll.

### 11. Migrations ausführen

```
sql/migrations/V001__disable_non_customers_pipelines.sql
```

Setzt alle Pipelines außer `customers` auf `is_active = 0` — während der Entwicklung werden nur die bereits implementierten Entities aktiviert.

---

## Pipeline manuell ausführen

Nach dem Setup kann der vollständige Lauf direkt in SSMS ausgelöst werden:

```sql
USE OlistDWH;
EXEC orchestration.sp_run_full_load @triggered_by = 'MANUAL';
```

Oder nur ein einzelner Layer:

```sql
EXEC orchestration.sp_run_layer @layer = 'RAW';
EXEC orchestration.sp_run_layer @layer = 'CLEANSED';
```

---

## Audit-Abfragen

Laufstatus aller Pipelines:
```sql
SELECT pipeline_id, layer, table_name, load_sequence, last_run_status, last_run_ts, last_batch_id
FROM orchestration.pipeline_config;
```

Letzter Job-Lauf:
```sql
SELECT TOP 1 * FROM audit.job_log ORDER BY start_ts DESC;
```

DQ-Probleme eines Batches:
```sql
SELECT table_name, column_name, issue, raw_value, COUNT(*) AS occurrences
FROM audit.dq_log
WHERE batch_id = '<batch_id>'
GROUP BY table_name, column_name, issue, raw_value
ORDER BY table_name, occurrences DESC;
```

Fehler eines Batches:
```sql
SELECT * FROM audit.error_log WHERE batch_id = '<batch_id>';
```
