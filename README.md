# Cetus backend — real data pipeline (v0.1)

A working ingestion pipeline for two **real, open, no-partnership-required**
data sources, plus a minimal page to verify it end to end.

## What this gives you today
- **Whale occurrence records** from OBIS (Ocean Biodiversity Information
  System) — real sighting/telemetry data contributed by research groups
  worldwide, filtered to a species and bounding box.
- **Sea-surface temperature** from NOAA ERDDAP — real, current ocean
  context data on a grid.

## What this does NOT give you
Continuous, single-whale movement tracks (the kind shown in the earlier
Cetus dashboard mockup). That specific data lives behind Argos/CLS, and
using it requires a data-sharing agreement with whoever deployed the
tags. That's a partnership conversation, not an engineering gap —
pursue it in parallel with building this.

## Setup
```bash
cd server
npm install
cp .env.example .env      # edit if you want a different region/species
npm start
```
Then open `http://localhost:8080` — it serves the check page in
`/public` and refreshes the OBIS + ERDDAP cache on the interval set in
`.env` (default hourly).

**Before deploying for real:** neither API has been called from this
environment (no outbound network access here to test with). Load the
page once against your real deployment and confirm both panels
populate. If OBIS or ERDDAP have changed their response shape since
this was written, the field names in `server.js` (`fetchWhaleOccurrences`
/ `fetchSeaSurfaceTemp`) are the place to fix it.

## Deploying
This is a plain Node/Express app — deploy it anywhere that runs Node
(Render, Railway, Fly.io, a VPS). It does **not** need any API keys for
the two sources currently wired up. Point your own domain at it and
serve the frontend from the same origin (already set up via
`express.static`) to avoid CORS complications.

## Roadmap
1. **Now:** real occurrence + ocean-context data (this repo).
2. **Restyle:** wire the earlier Cetus dashboard's visual design to
   these endpoints instead of its hardcoded arrays — cosmetic work,
   not a new data problem.
3. **Population trend:** NOAA stock assessment reports aren't a clean
   API — this needs either manual periodic updates or a scraper against
   published PDF reports.
4. **Real tag tracks:** requires an Argos/CLS data-sharing agreement
   with a tagging research partner. Business development, not code.
5. **Imagery:** a Maxar (or equivalent) commercial contract, once
   there's a client need specific enough to justify the cost.
