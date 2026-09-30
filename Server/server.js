/**
 * Cetus backend
 * -------------
 * Pulls REAL data from two open sources on a schedule, caches it in memory,
 * and serves it to the frontend over a small REST API.
 *
 *   1. OBIS (Ocean Biodiversity Information System) — whale occurrence /
 *      telemetry records. Public API, no key required.
 *      Docs: https://api.obis.org/
 *
 *   2. NOAA ERDDAP — gridded sea-surface-temperature data. Public API,
 *      no key required.
 *      Docs: https://coastwatch.pfeg.noaa.gov/erddap/index.html
 *
 * IMPORTANT: this file has NOT been run against the live OBIS/ERDDAP
 * endpoints (the environment that wrote it has no outbound network
 * access). The request shapes below match each service's documented
 * query conventions, but verify one real response from each endpoint
 * in a browser or with curl before relying on this in production —
 * field names or dataset IDs can drift over time.
 *
 * Neither source gives continuous single-whale movement tracks. That
 * requires a data-sharing agreement with whoever deployed the Argos
 * tags — a partnership question, not something this pipeline solves.
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const cron = require('node-cron');

const app = express();
app.use(cors());

const PORT = process.env.PORT || 8080;

const BBOX = {
  minLon: parseFloat(process.env.BBOX_MIN_LON || '-80'),
  minLat: parseFloat(process.env.BBOX_MIN_LAT || '18'),
  maxLon: parseFloat(process.env.BBOX_MAX_LON || '-66'),
  maxLat: parseFloat(process.env.BBOX_MAX_LAT || '45'),
};

const SPECIES_LIST = (process.env.SPECIES_SCIENTIFIC_NAME || 'Megaptera novaeangliae')
  .split(',').map(s => s.trim()).filter(Boolean);
const OBIS_LOOKBACK_DAYS = parseInt(process.env.OBIS_LOOKBACK_DAYS || '365', 10);
const ERDDAP_DATASET_ID = process.env.ERDDAP_DATASET_ID || 'jplMURSST41';
const ERDDAP_BASE_URL = process.env.ERDDAP_BASE_URL || 'https://coastwatch.pfeg.noaa.gov/erddap';
const REFRESH_MINUTES = parseInt(process.env.REFRESH_INTERVAL_MINUTES || '60', 10);

const SST_HISTORY_MONTHS = parseInt(process.env.SST_HISTORY_MONTHS || '13', 10);

// BOEM's own public GIS service for active Gulf of Mexico oil & gas
// leases. This is REAL government data, not a sample -- but note its
// scope: BOEM only manages US waters, and this specific service covers
// the Gulf of Mexico region only, not US-wide or global drilling activity.
// Verify this is still live at the URL below before depending on it --
// Esri REST services occasionally get reorganized.
// Browsable at: https://services1.arcgis.com/qr14biwnHA6Vis6l/ArcGIS/rest/services/Platforms_Pipelines_ActiveLease/FeatureServer
const BOEM_LEASES_URL = process.env.BOEM_LEASES_URL ||
  'https://services1.arcgis.com/qr14biwnHA6Vis6l/ArcGIS/rest/services/Platforms_Pipelines_ActiveLease/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson';

// ---- in-memory cache -------------------------------------------------
// Swap this for a real database (Postgres, SQLite, etc.) once this is
// past the prototype stage -- an in-memory cache resets on every deploy
// and doesn't survive multiple server instances.
const cache = {
  whaleOccurrences: { updatedAt: null, data: [] },
  seaSurfaceTemp: { updatedAt: null, data: [] },
  drillingLeases: { updatedAt: null, data: [] },
};

// ---- OBIS: whale occurrence / telemetry records -----------------------
// Queries OBIS once per species in SPECIES_LIST and merges the results.
async function fetchWhaleOccurrences() {
  const wkt = `POLYGON((` +
    `${BBOX.minLon} ${BBOX.minLat},` +
    `${BBOX.maxLon} ${BBOX.minLat},` +
    `${BBOX.maxLon} ${BBOX.maxLat},` +
    `${BBOX.minLon} ${BBOX.maxLat},` +
    `${BBOX.minLon} ${BBOX.minLat}))`;

  const startDate = new Date(Date.now() - OBIS_LOOKBACK_DAYS * 86400000)
    .toISOString().slice(0, 10);

  const perSpeciesResults = await Promise.all(SPECIES_LIST.map(async (species) => {
    const url = new URL('https://api.obis.org/v3/occurrence');
    url.searchParams.set('scientificname', species);
    url.searchParams.set('geometry', wkt);
    url.searchParams.set('startdate', startDate);
    url.searchParams.set('size', '500');

    const res = await fetch(url.toString(), {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1)' },
    });
    if (!res.ok) {
      console.error(`[obis] request failed for ${species}: ${res.status}`);
      return [];
    }
    const body = await res.json();

    return (body.results || [])
      .filter(r => r.decimalLatitude != null && r.decimalLongitude != null)
      .map(r => ({
        id: r.id || r.occurrenceID,
        species: r.scientificName || species,
        lat: r.decimalLatitude,
        lon: r.decimalLongitude,
        date: r.eventDate || r.date_year || null,
        dataset: r.datasetName || null,
        basisOfRecord: r.basisOfRecord || null,
      }));
  }));

  return perSpeciesResults.flat();
}

// ---- NOAA ERDDAP: sea-surface temperature grid ------------------------
async function fetchSeaSurfaceTemp() {
  // Samples SST on a coarse grid across the bounding box. ERDDAP griddap
  // queries take [(time)][(lat)][(lon)] ranges; adjust stride/step as
  // needed once you've confirmed the dataset's actual grid resolution.
  const latStep = parseInt(process.env.ERDDAP_LAT_STRIDE || '40', 10);
  const lonStep = parseInt(process.env.ERDDAP_LON_STRIDE || '40', 10);
  const points = [];
  const url =
    `${ERDDAP_BASE_URL}/griddap/${ERDDAP_DATASET_ID}.json` +
    `?analysed_sst[(last)][(${BBOX.minLat}):${latStep}:(${BBOX.maxLat})]` +
    `[(${BBOX.minLon}):${lonStep}:(${BBOX.maxLon})]`;

  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1; +https://onrender.com)',
      'Accept': 'application/json',
    },
  });
  if (!res.ok) throw new Error(`ERDDAP request failed: ${res.status} ${res.statusText}`);
  const body = await res.json();

  const rows = (body.table && body.table.rows) || [];
  const colNames = (body.table && body.table.columnNames) || [];
  const latIdx = colNames.indexOf('latitude');
  const lonIdx = colNames.indexOf('longitude');
  const sstIdx = colNames.indexOf('analysed_sst');
  const timeIdx = colNames.indexOf('time');

  for (const row of rows) {
    points.push({
      lat: row[latIdx],
      lon: row[lonIdx],
      sstCelsius: row[sstIdx],
      time: timeIdx >= 0 ? row[timeIdx] : null,
    });
  }
  return points;
}

// ---- NOAA ERDDAP: SST grid for one specific historical date ------------
// Same dataset/grid as fetchSeaSurfaceTemp above, but queries a specific
// date instead of "last" -- this is what lets us match temperature to
// the actual date of each sighting instead of only "right now".
async function fetchSeaSurfaceTempForDate(dateISO) {
  const latStep = parseInt(process.env.ERDDAP_LAT_STRIDE || '40', 10);
  const lonStep = parseInt(process.env.ERDDAP_LON_STRIDE || '40', 10);
  const url =
    `${ERDDAP_BASE_URL}/griddap/${ERDDAP_DATASET_ID}.json` +
    `?analysed_sst[(${dateISO})][(${BBOX.minLat}):${latStep}:(${BBOX.maxLat})]` +
    `[(${BBOX.minLon}):${lonStep}:(${BBOX.maxLon})]`;

  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1; +https://onrender.com)',
      'Accept': 'application/json',
    },
  });
  if (!res.ok) throw new Error(`ERDDAP historical request failed for ${dateISO}: ${res.status}`);
  const body = await res.json();

  const rows = (body.table && body.table.rows) || [];
  const colNames = (body.table && body.table.columnNames) || [];
  const latIdx = colNames.indexOf('latitude');
  const lonIdx = colNames.indexOf('longitude');
  const sstIdx = colNames.indexOf('analysed_sst');

  return rows.map(row => ({ lat: row[latIdx], lon: row[lonIdx], sstCelsius: row[sstIdx] }));
}

// Fetches one SST grid snapshot per month for the last SST_HISTORY_MONTHS
// months (mid-month date, to sidestep month-length edge cases), keyed by
// 'YYYY-MM'. Runs sequentially rather than in parallel to avoid hammering
// ERDDAP with a dozen+ simultaneous requests -- this makes a refresh cycle
// slower but is politer to a public, unauthenticated service.
async function fetchHistoricalSSTByMonth() {
  const monthly = {};
  const now = new Date();
  for (let i = 0; i < SST_HISTORY_MONTHS; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 15);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const iso = d.toISOString().slice(0, 10) + 'T00:00:00Z';
    try {
      monthly[key] = await fetchSeaSurfaceTempForDate(iso);
      console.log(`[sst-history] ${key}: ${monthly[key].length} points`);
    } catch (err) {
      console.error(`[sst-history] failed for ${key}:`, err.message || err);
      monthly[key] = [];
    }
  }
  return monthly;
}

function nearestPointSST(points, lat, lon) {
  let best = null, bestD = Infinity;
  for (const p of points) {
    if (p.sstCelsius == null) continue;
    const dd = (p.lat - lat) ** 2 + (p.lon - lon) ** 2;
    if (dd < bestD) { bestD = dd; best = p; }
  }
  return best;
}

// Mutates each occurrence in place, adding historicalSstCelsius --
// the nearest grid reading from the SST snapshot for that occurrence's
// own month, rather than today's temperature.
function attachHistoricalSST(occurrences, monthlySST) {
  occurrences.forEach(o => {
    if (!o.date) { o.historicalSstCelsius = null; return; }
    const d = new Date(o.date);
    if (isNaN(d)) { o.historicalSstCelsius = null; return; }
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const points = monthlySST[key];
    if (!points || !points.length) { o.historicalSstCelsius = null; return; }
    const nearest = nearestPointSST(points, o.lat, o.lon);
    o.historicalSstCelsius = nearest ? nearest.sstCelsius : null;
  });
}

// ---- BOEM: active Gulf of Mexico oil & gas lease polygons --------------
async function fetchDrillingLeases() {
  console.log('[leases] requesting:', BOEM_LEASES_URL);
  const res = await fetch(BOEM_LEASES_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1)' },
  });
  console.log('[leases] response status:', res.status, res.statusText);
  if (!res.ok) {
    const bodyText = await res.text();
    console.log('[leases] error body:', bodyText.slice(0, 500));
    throw new Error(`BOEM leases request failed: ${res.status}`);
  }
  const geojson = await res.json();
  const features = geojson.features || [];

  // Normalize to a lightweight shape: just an outer ring of [lon,lat]
  // pairs per polygon, dropping most attribute fields to keep the
  // response small (a lease area can have dozens of fields we don't use).
  const polygons = [];
  features.forEach(f => {
    const geom = f.geometry;
    if (!geom) return;
    const props = f.properties || {};
    const leaseId = props.LEASE_NUMB || props.LEASE_NUMBER || props.LEASE_NO || null;
    if (geom.type === 'Polygon') {
      polygons.push({ id: leaseId, ring: geom.coordinates[0] });
    } else if (geom.type === 'MultiPolygon') {
      geom.coordinates.forEach(poly => {
        polygons.push({ id: leaseId, ring: poly[0] });
      });
    }
  });
  return polygons;
}

// ---- refresh cycle ------------------------------------------------------
async function refreshAll() {
  try {
    cache.whaleOccurrences.data = await fetchWhaleOccurrences();
    cache.whaleOccurrences.updatedAt = new Date().toISOString();
    console.log(`[refresh] OBIS occurrences: ${cache.whaleOccurrences.data.length} records`);
  } catch (err) {
    console.error('[refresh] OBIS fetch failed:', err.message);
  }

  try {
    cache.seaSurfaceTemp.data = await fetchSeaSurfaceTemp();
    cache.seaSurfaceTemp.updatedAt = new Date().toISOString();
    console.log(`[refresh] ERDDAP SST points: ${cache.seaSurfaceTemp.data.length}`);
  } catch (err) {
    console.error('[refresh] ERDDAP fetch failed — raw error object below:');
    console.error(err);
  }

  // Historical (date-matched) SST -- depends on whaleOccurrences already
  // being populated above, since it attaches values onto those records.
  try {
    const monthlySST = await fetchHistoricalSSTByMonth();
    attachHistoricalSST(cache.whaleOccurrences.data, monthlySST);
    const matched = cache.whaleOccurrences.data.filter(o => o.historicalSstCelsius != null).length;
    console.log(`[refresh] historical SST matched to ${matched}/${cache.whaleOccurrences.data.length} occurrences`);
  } catch (err) {
    console.error('[refresh] historical SST matching failed:', err.message || err);
  }

  try {
    cache.drillingLeases.data = await fetchDrillingLeases();
    cache.drillingLeases.updatedAt = new Date().toISOString();
    console.log(`[refresh] BOEM leases: ${cache.drillingLeases.data.length} polygons`);
  } catch (err) {
    console.error('[refresh] BOEM leases fetch failed:', err.message || err);
  }
}

// ---- API ------------------------------------------------------------
app.get('/api/whales', (req, res) => {
  res.json(cache.whaleOccurrences);
});

app.get('/api/ocean', (req, res) => {
  res.json(cache.seaSurfaceTemp);
});

app.get('/api/leases', (req, res) => {
  res.json(cache.drillingLeases);
});

app.get('/api/status', (req, res) => {
  res.json({
    bbox: BBOX,
    species: SPECIES_LIST,
    whaleOccurrences: {
      updatedAt: cache.whaleOccurrences.updatedAt,
      count: cache.whaleOccurrences.data.length,
    },
    seaSurfaceTemp: {
      updatedAt: cache.seaSurfaceTemp.updatedAt,
      count: cache.seaSurfaceTemp.data.length,
    },
    drillingLeases: {
      updatedAt: cache.drillingLeases.updatedAt,
      count: cache.drillingLeases.data.length,
      scope: 'BOEM Gulf of Mexico active leases only — not US-wide or global',
    },
    note: 'No continuous single-whale tracks here — that needs an Argos data-sharing agreement.',
  });
});

app.use(express.static('../Public'));

app.listen(PORT, async () => {
  console.log(`Cetus backend listening on :${PORT}`);
  await refreshAll(); // populate cache on boot
  cron.schedule(`*/${REFRESH_MINUTES} * * * *`, refreshAll);
});
