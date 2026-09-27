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

const SPECIES = process.env.SPECIES_SCIENTIFIC_NAME || 'Megaptera novaeangliae';
const OBIS_LOOKBACK_DAYS = parseInt(process.env.OBIS_LOOKBACK_DAYS || '365', 10);
const ERDDAP_DATASET_ID = process.env.ERDDAP_DATASET_ID || 'jplMURSST41';
const REFRESH_MINUTES = parseInt(process.env.REFRESH_INTERVAL_MINUTES || '60', 10);

const cache = {
  whaleOccurrences: { updatedAt: null, data: [] },
  seaSurfaceTemp: { updatedAt: null, data: [] },
};

async function fetchWhaleOccurrences() {
  const wkt = `POLYGON((` +
    `${BBOX.minLon} ${BBOX.minLat},` +
    `${BBOX.maxLon} ${BBOX.minLat},` +
    `${BBOX.maxLon} ${BBOX.maxLat},` +
    `${BBOX.minLon} ${BBOX.maxLat},` +
    `${BBOX.minLon} ${BBOX.minLat}))`;

  const startDate = new Date(Date.now() - OBIS_LOOKBACK_DAYS * 86400000)
    .toISOString().slice(0, 10);

  const url = new URL('https://api.obis.org/v3/occurrence');
  url.searchParams.set('scientificname', SPECIES);
  url.searchParams.set('geometry', wkt);
  url.searchParams.set('startdate', startDate);
  url.searchParams.set('size', '500');

  const res = await fetch(url.toString(), {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1)' },
  });
  if (!res.ok) throw new Error(`OBIS request failed: ${res.status}`);
  const body = await res.json();

  return (body.results || [])
    .filter(r => r.decimalLatitude != null && r.decimalLongitude != null)
    .map(r => ({
      id: r.id || r.occurrenceID,
      species: r.scientificName || SPECIES,
      lat: r.decimalLatitude,
      lon: r.decimalLongitude,
      date: r.eventDate || r.date_year || null,
      dataset: r.datasetName || null,
      basisOfRecord: r.basisOfRecord || null,
    }));
}

async function fetchSeaSurfaceTemp() {
  const latStep = 2, lonStep = 2;
  const points = [];
  const url =
    `https://coastwatch.pfeg.noaa.gov/erddap/griddap/${ERDDAP_DATASET_ID}.json` +
    `?analysed_sst[(last)][(${BBOX.minLat}):${latStep}:(${BBOX.maxLat})]` +
    `[(${BBOX.minLon}):${lonStep}:(${BBOX.maxLon})]`;

  console.log('[erddap] requesting:', url);

  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1; +https://onrender.com)',
      'Accept': 'application/json',
    },
  });

  console.log('[erddap] response status:', res.status, res.statusText);

  if (!res.ok) {
    const bodyText = await res.text();
    console.log('[erddap] error body:', bodyText.slice(0, 500));
    throw new Error(`ERDDAP request failed: ${res.status} ${res.statusText}`);
  }
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
    console.error('[refresh] ERDDAP fetch failed — full error dump below');
    console.error(err);
  }
}

app.get('/api/whales', (req, res) => {
  res.json(cache.whaleOccurrences);
});

app.get('/api/ocean', (req, res) => {
  res.json(cache.seaSurfaceTemp);
});

app.get('/api/status', (req, res) => {
  res.json({
    bbox: BBOX,
    species: SPECIES,
    whaleOccurrences: {
      updatedAt: cache.whaleOccurrences.updatedAt,
      count: cache.whaleOccurrences.data.length,
    },
    seaSurfaceTemp: {
      updatedAt: cache.seaSurfaceTemp.updatedAt,
      count: cache.seaSurfaceTemp.data.length,
    },
    note: 'No continuous single-whale tracks here — that needs an Argos data-sharing agreement.',
  });
});

app.use(express.static('../Public'));

app.listen(PORT, async () => {
  console.log(`Cetus backend listening on :${PORT}`);
  await refreshAll();
  cron.schedule(`*/${REFRESH_MINUTES} * * * *`, refreshAll);
});
