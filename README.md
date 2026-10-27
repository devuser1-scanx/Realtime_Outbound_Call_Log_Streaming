# Realtime_Outbound_Call_Log_Streaming

Streams outbound web-dialer call events from the Cloud SQL `call_logs` table to a Google Cloud Pub/Sub topic for a third-party consumer. Cloud Run service: `realtime-outbound-call-log-streaming`.

The service is **read-only on `call_logs`** (never writes to it) and owns one table, `dialer_stream_sent`, used to track what has been delivered.

## How it works

Every `POLL_INTERVAL_SECONDS` a single instance (guarded by a Postgres advisory lock) selects unsent `call_logs` rows by primary key, filtered to `direction = 'outbound'`, not an excluded staff identity, and `id > START_AFTER_CALL_LOG_ID`. Each becomes one Pub/Sub message; after the publish future succeeds the row is recorded in `dialer_stream_sent`. Failed publishes are retried on the next loop. `event_id` is a deterministic uuid5 of the call log id, so a re-send after a crash can be de-duplicated by the consumer. Delivery is therefore at-least-once.

Message data (JSON, matches the topic's Avro schema): `event_id`, `event_type`, `occurred_at`, `call_log_id`, `direction`, `to_number`. Attributes: `event_id`, `event_type=call.initiated`, `schema_version=1`.

`GET /healthz` returns 200 `{"ok": true, "last_success", "pending_estimate"}`, or 503 if no successful loop iteration in 120 s.

## Configuration

| Name | Example | Notes |
|---|---|---|
| `GCP_PROJECT_ID` | `vernal-maker-473121-k4` | |
| `PUBSUB_TOPIC` | `call-events` | |
| `INSTANCE_CONNECTION_NAME` | `vernal-maker-473121-k4:us-central1:<instance>` | Cloud SQL instance |
| `DB_NAME` | | same DB as `call_logs` |
| `DB_USER` | `outbound_call_streaming` | |
| `DB_PASSWORD` | | secret `outbound-call-streaming-db-password` |
| `START_AFTER_CALL_LOG_ID` | `1389` | `SELECT MAX(id) FROM call_logs` at go-live |
| `EXCLUDED_STAFF` | `clinic-tablet` | comma-separated, case-insensitive |
| `POLL_INTERVAL_SECONDS` | `1` | |
| `BATCH_SIZE` | `500` | |
| `LOOKBACK_ROWS` | `1000` | re-checks recent ids for late commits |
| `LOG_LEVEL` | `INFO` | |

Missing required variables make the service fail at startup. Pub/Sub and Cloud SQL use the Cloud Run service account (ADC); there are no key files.

## One-time setup (operator)

```bash
PROJECT=vernal-maker-473121-k4
SA=outbound-call-streaming@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts create outbound-call-streaming --project $PROJECT
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA --role=roles/cloudsql.client
gcloud pubsub topics add-iam-policy-binding call-events --member=serviceAccount:$SA --role=roles/pubsub.publisher --project $PROJECT

printf '%s' '<db password>' | gcloud secrets create outbound-call-streaming-db-password --data-file=- --project $PROJECT
gcloud secrets add-iam-policy-binding outbound-call-streaming-db-password \
  --member=serviceAccount:$SA --role=roles/secretmanager.secretAccessor --project $PROJECT
```

Database user (run as an admin in psql):

```sql
CREATE USER outbound_call_streaming WITH PASSWORD '<db password>';
GRANT CONNECT ON DATABASE <db> TO outbound_call_streaming;
GRANT USAGE ON SCHEMA public TO outbound_call_streaming;
GRANT SELECT (id, staff_phone, patient_number, direction, created_at) ON call_logs TO outbound_call_streaming;
GRANT CREATE ON SCHEMA public TO outbound_call_streaming;   -- only so it can create dialer_stream_sent; or create it as admin and grant below
-- if created by admin instead (run migrations/001_dialer_stream_sent.sql, then):
-- GRANT SELECT, INSERT ON dialer_stream_sent TO outbound_call_streaming;
```

The Pub/Sub topic, schema, dead-letter topic and partner subscription are created separately (see PLAN.md step 2).

## Deploy

```bash
gcloud run deploy realtime-outbound-call-log-streaming \
  --source . --region us-central1 --project $PROJECT \
  --service-account $SA \
  --no-allow-unauthenticated \
  --min-instances 1 --max-instances 1 --no-cpu-throttling \
  --add-cloudsql-instances <INSTANCE_CONNECTION_NAME> \
  --set-env-vars GCP_PROJECT_ID=$PROJECT,PUBSUB_TOPIC=call-events,INSTANCE_CONNECTION_NAME=<...>,DB_NAME=<...>,DB_USER=outbound_call_streaming,START_AFTER_CALL_LOG_ID=<max id>,EXCLUDED_STAFF=clinic-tablet \
  --set-secrets DB_PASSWORD=outbound-call-streaming-db-password:latest
```

Optionally configure a liveness probe on `/healthz` so Cloud Run restarts a stuck instance.

## Verify

1. Place one outbound call from the dialer as a non-`clinic-tablet` user. Within ~2 s exactly one message should arrive on a debug subscription matching the format above, and `SELECT * FROM dialer_stream_sent ORDER BY call_log_id DESC LIMIT 1;` shows the row.
2. A `clinic-tablet` call and an inbound call produce no message.
3. Stop the service, place a call, restart: that call's message is delivered once.
4. Cloud Run logs (JSON, with `call_log_id` / `event_id`) never contain phone numbers.

## Local development

```bash
python -m venv venv && venv/Scripts/activate   # or source venv/bin/activate
pip install -r requirements.txt
pytest
```

Container scan: `docker build -t rocls . && trivy image --severity HIGH,CRITICAL --ignore-unfixed rocls`.

## Rollback

- **Stop streaming:** `gcloud run services update realtime-outbound-call-log-streaming --min-instances 0 --max-instances 0 --region us-central1 --project $PROJECT` (or delete the service). Unsent rows are picked up when it is restarted, because anything not in `dialer_stream_sent` is retried.
- **Previous revision:** `gcloud run services update-traffic realtime-outbound-call-log-streaming --to-revisions <REVISION>=100 --region us-central1 --project $PROJECT`.
- **Full removal:** delete the service, then `DROP TABLE dialer_stream_sent;` and `DROP USER outbound_call_streaming;` (after revoking grants). `call_logs` is untouched throughout.
- **Resend a specific call:** `DELETE FROM dialer_stream_sent WHERE call_log_id = <id>;` (the consumer de-duplicates by the same `event_id`).
