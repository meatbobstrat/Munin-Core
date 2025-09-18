Quick start

Clone:
git clone https://github.com/meatbobstrat/Munin-Core

cd Munin-Core

Create a venv:
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

Configure ingestion (configs/ingest.config.json):
{
"watch_dir": "logs/incoming",
"processing_dir": "logs/processing",
"quarantine_dir": "logs/quarantine",
"delete_after_ingest": true,
"batch_size": 1000,
"source_host": "demo-host",
"source_app": "demo-app"
}

Run API + watcher:
docker compose up

munin-api: receives and stores events

munin-ingestor: watches folders and pushes logs to API

Security model

Reverse proxy (Keycloak/Authelia) with AD or Azure auth + MFA.

Roles: admin, operator, ingest.

Ingest: HMAC-signed batches.

Sandboxed LLM notes table for self-reference (EchoTime).

Audit logging + metrics endpoints.

Hardened host (systemd lockdown, UFW, fail2ban).

Where Munin-Core fits

Legacy SIEM pipeline:

[Ingest] → [Pre-processing & Policy Filtering] → [Correlation] → [Alarm Risk Assessment] → [Dashboards]

Munin-Core replaces the middle:

[Standalone Ingestors] → [API + Storage] → [AI Enrichment + Summaries + Notes] → [Chat + Canvas Visibility]

Roadmap

Sprint 1 (done): Ingestion MVP

Standalone ingestor containers

File-watcher ingestion (CSV, EVTX, raw text)

Batch and single-event API endpoints

Basic DB schema (logs, normalized_events)

Sprint 2 (in progress): Security & Enrichment

HMAC-signed ingestion

Retry/buffer on failure

File handling (processed + quarantine)

EVTX parser validation and JSON/syslog support

Role-based auth integration

Extended schema (alerts, echotime) groundwork

Sprint 3: Intelligence & Dashboard

Alerts surfaced via API/UI

Search and saved queries

GPT summaries and annotations

Dashboard + visualization layer

Future backlog:

Parser extensibility (JSON, syslog, etc.)

Data retention & DB auto-pruning

Quarantine + validation workflows

Web site scraping/monitoring with login support

Vector embeddings for semantic search

WORM cold-store mode

Export controls and row-level ABAC

CI pipeline: build/test Docker images

License

MIT

Munin-Core vs. Traditional SIEMs
Feature	Splunk/Elastic/Sentinel	Munin-Core
Ingestion	Complex, licensed by volume	Lossless, standalone drop-in ingest
Correlation/Rules	Static rules, endless tuning	AI-driven enrichment & context
UI	Dashboards & queries	Chat + canvas (ask, answer, annotate)
Memory	None	AI sandbox notes (EchoTime)
Security	Add-on MFA, complex RBAC	MFA, AD/Azure, HMAC ingest (by default)
Openness	Proprietary	Open source core
Pricing	$$$$ (volume-based)	Free core; enterprise packs optional