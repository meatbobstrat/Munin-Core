[![CI](https://github.com/meatbobstrat/Munin-Core/actions/workflows/ci.yml/badge.svg)](https://github.com/meatbobstrat/Munin-Core/actions/workflows/ci.yml)

# Munin-Core

Every org should have access to a good SIEM.

AI-assisted log ingestion & visibility engine.  
Replaces the brittle "rules & correlation" layer of legacy SIEMs with GPT-powered context, summaries, and annotations.

---

## Why Munin-Core?
Traditional SIEMs rely on static schemas, correlation rules, and alarm scoring.  
That means endless tuning, blind spots, and false positives.

Munin-Core takes a different path:
- **Lossless ingestion** – every line stored, no dedup suppression.
- **Standalone ingestors** – lightweight containers that watch sources and push to the API.
- **AI enrichment** – GPT adds summaries, highlights secrets/PII, detects anomalies.
- **Chat-driven interface** – ask natural questions, get contextual answers.
- **Memory sandbox** – the AI keeps its own notes, anchored in EchoTime epochs.
- **Security by design** – MFA, AD/Azure auth, HMAC-signed ingestion, audit logs, hardened host.

---

## Quick start
1. Clone:
   git clone https://github.com/meatbobstrat/Munin-Core
   cd Munin-Core

2. Create a venv:
   python -m venv venv && source venv/bin/activate
   pip install -r requirements.txt

3. Configure ingestion (`configs/ingest.config.json`):
   {
     "watch_dir": "logs/incoming",
     "processing_dir": "logs/processing",
     "quarantine_dir": "logs/quarantine",
     "delete_after_ingest": true,
     "batch_size": 1000,
     "source_host": "demo-host",
     "source_app": "demo-app"
   }

4. Run API + ingestor:
   docker compose up

   - munin-api: receives and stores events  
   - munin-ingestor: watches folders and pushes logs to API

---

## Security model
- Reverse proxy (Keycloak/Authelia) with AD or Azure auth + MFA.
- Roles: `admin`, `operator`, `ingest`.
- Ingest: HMAC-signed batches.
- Sandboxed LLM `notes` table for self-reference (EchoTime).
- Audit logging + metrics endpoints.
- Hardened host (systemd lockdown, UFW, fail2ban).

---

## Where Munin-Core fits
Legacy SIEM pipeline:

[Ingest] → [Pre-processing & Policy Filtering] → [Correlation] → [Alarm Risk Assessment] → [Dashboards]

Munin-Core replaces the middle:

[Ingest] → [AI Enrichment + Summaries + Notes] → [Chat + Canvas Visibility]

---

## Roadmap
- **Sprint 1 (done):** Ingestion MVP.
- **Sprint 2 (in progress):** Standalone ingestors, security slice, annotations, notes.
- **Sprint 3:** Alerts, search, GPT summaries, dashboard.
- **Future backlog:**
  - Web site scraping/monitoring with login support.
  - Storage quota enforcement (DB auto-pruning).
  - Saved searches & alerts.
  - Vector embeddings for semantic search.
  - WORM cold-store mode.
  - Export controls and row-level ABAC.
  - Multi-source ingestion orchestration.
  - Heartbeat + presence monitoring.

---

## License
MIT

## Munin-Core vs. Traditional SIEMs

| Feature                | Splunk/Elastic/Sentinel           | Munin-Core                              |
|------------------------|-----------------------------------|-----------------------------------------|
| Ingestion              | Complex, licensed by volume       | Lossless, simple drop-in folder ingest   |
| Correlation/Rules      | Static rules, endless tuning      | AI-driven enrichment & context           |
| UI                     | Dashboards & queries              | Chat + canvas (ask, answer, annotate)    |
| Memory                 | None                              | AI sandbox notes (EchoTime)              |
| Security               | Add-on MFA, complex RBAC          | MFA, AD/Azure, HMAC ingest (by default)  |
| Openness               | Proprietary                       | Open source core                         |
| Pricing                | $$$$ (volume-based)               | Free core; enterprise packs optional     |
