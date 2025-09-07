# Munin-Core

AI-assisted log ingestion & visibility engine.

## What it does
- Ingests logs dropped into a folder (any cadence you like).
- Normalizes and stores every line (no dedup loss).
- Adds GPT summaries + secret/PII annotations (configurable).
- Lets you explore via a **chat window + canvas UI** (operators ask, AI answers).
- Designed with **safety-first**: MFA, AD/Azure auth, audit logs, hardened host.

## Quick start
1. `git clone https://github.com/meatbobstrat/Munin-Core`
2. `python -m venv venv && source venv/bin/activate`
3. `pip install -r requirements.txt`
4. Create `configs/ingest.config.json`:
   ```json
   {
     "watch_dir": "logs/incoming",
     "processing_dir": "logs/processing",
     "quarantine_dir": "logs/quarantine",
     "delete_after_ingest": true,
     "batch_size": 1000,
     "source_host": "demo-host",
     "source_app": "demo-app"
   }
