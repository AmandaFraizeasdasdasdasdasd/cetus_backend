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
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const cron = require('node-cron');
const topojson = require('topojson-client');
const worldLandTopology = require('world-atlas/land-110m.json');

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
  'https://services1.arcgis.com/qr14biwnHA6Vis6l/ArcGIS/rest/services/Platforms_Pipelines_ActiveLease/FeatureServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=geojson';

// NOAA Fisheries' own public GIS service for North Atlantic Right Whale
// Seasonal Management Areas -- real mandatory vessel speed-restriction
// zones, not a sample. Covers the US Atlantic seaboard only.
const RIGHT_WHALE_SMA_URL = process.env.RIGHT_WHALE_SMA_URL ||
  'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Seasonal_Management_Areas/FeatureServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=geojson';

// NOAA's Marine Protected Areas Inventory -- real US MPA boundaries with
// protection-level classification. US-only, like the two above.
const MPA_BOUNDARIES_URL = process.env.MPA_BOUNDARIES_URL ||
  'https://services1.arcgis.com/eGSDp8lpKe5izqVc/ArcGIS/rest/services/Marine_Protected_Areas__MPAIs_/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson';

// NOAA Office for Coastal Management / MarineCadastre -- US Navy and other
// military Operating Areas (OPAREAs), where training exercises and system
// tests (including sonar and live explosives) are routinely conducted.
// US waters plus Hawaii and Guam. Boundaries only: exercise schedules are
// not published as a live feed.
const MILITARY_OPAREA_URL = process.env.MILITARY_OPAREA_URL ||
  'https://coast.noaa.gov/arcgis/rest/services/Hosted/MilitaryOperatingAreaBoundaries/FeatureServer/0/query?where=1%3D1&outFields=featurename,typedomain,installationidentifier&outSR=4326&f=geojson';

// ---- in-memory cache -------------------------------------------------
// Swap this for a real database (Postgres, SQLite, etc.) once this is
// past the prototype stage -- an in-memory cache resets on every deploy
// and doesn't survive multiple server instances.
const cache = {
  whaleOccurrences: { updatedAt: null, data: [] },
  seaSurfaceTemp: { updatedAt: null, data: [] },
  drillingLeases: { updatedAt: null, data: [] },
  rightWhaleSMA: { updatedAt: null, data: [] },
  mpaBoundaries: { updatedAt: null, data: [] },
  militaryAreas: { updatedAt: null, data: [] },
  speciesImages: { updatedAt: null, data: {} },
  populationStatus: { updatedAt: null, data: {} },
};

// ---- Wikipedia: one representative image per tracked species -----------
// Fetches dynamically for whatever's in SPECIES_LIST, so adding a new
// species to track (via the env var) automatically picks up an image --
// no hardcoded per-species list to maintain. Wikipedia's REST summary API
// follows redirects, so a scientific binomial name (e.g. "Megaptera
// novaeangliae") correctly resolves to the common-name article and its
// lead image in essentially all cases for well-known species.
async function fetchSpeciesImage(scientificName) {
  const title = encodeURIComponent(scientificName.trim().replace(/ /g, '_'));
  const url = `https://en.wikipedia.org/api/rest_v1/page/summary/${title}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'CetusBackend/0.1 (whale monitoring dashboard)' },
  });
  if (!res.ok) throw new Error(`Wikipedia summary request failed: ${res.status}`);
  const body = await res.json();
  const image = body.thumbnail || body.originalimage || null;
  return {
    imageUrl: image ? image.source : null,
    pageUrl: (body.content_urls && body.content_urls.desktop && body.content_urls.desktop.page) || null,
    commonName: body.title || scientificName,
  };
}

async function fetchAllSpeciesImages() {
  const result = {};
  for (const species of SPECIES_LIST) {
    try {
      result[species] = await fetchSpeciesImage(species);
      console.log(`[species-image] ${species} -> ${result[species].imageUrl ? 'found' : 'no image on page'}`);
    } catch (err) {
      console.error(`[species-image] failed for ${species}:`, err.message || err);
      result[species] = { imageUrl: null, pageUrl: null, commonName: species };
    }
  }
  return result;
}

// ---- IUCN Red List: conservation status & population trend -------------
// Real, authoritative per-species data -- but requires a free API token
// the user must request themselves (https://api.iucnredlist.org/users/sign_up).
// Skips gracefully (logs once, returns empty) if no token is configured.
// This gives a trend DIRECTION (increasing/decreasing/stable/unknown) and
// a conservation category, not a historical chart with exact numbers --
// IUCN doesn't publish structured time-series abundance data either.
//
// IMPORTANT: the exact response shape of IUCN's v4 API has not been
// verified against a live token from this environment (no internet access
// here to test with a real key). The two-step flow (look up taxon, then
// fetch its latest assessment) and field names below match the publicly
// documented structure, but if this comes back empty once a real token is
// in place, check the raw response logged below and adjust field access
// accordingly -- same troubleshooting pattern as the other integrations.
const IUCN_API_TOKEN = process.env.IUCN_API_TOKEN || null;
const IUCN_BASE_URL = 'https://apiv4.iucnredlist.org/api/v4';

async function iucnFetch(path) {
  const url = `${IUCN_BASE_URL}${path}`;
  const res = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${IUCN_API_TOKEN}`,
      'User-Agent': 'CetusBackend/0.1 (whale monitoring dashboard)',
    },
  });
  if (!res.ok) {
    const bodyText = await res.text();
    console.log(`[iucn] error body for ${path}:`, bodyText.slice(0, 400));
    throw new Error(`IUCN request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchIUCNStatus(scientificName) {
  const parts = scientificName.trim().split(/\s+/);
  const genus_name = parts[0];
  const species_name = parts.slice(1).join(' ');

  const taxonResp = await iucnFetch(
    `/taxa/scientific_name?genus_name=${encodeURIComponent(genus_name)}&species_name=${encodeURIComponent(species_name)}`
  );
  console.log(`[iucn] taxon lookup for ${scientificName}:`, JSON.stringify(taxonResp).slice(0, 300));

  const assessments = taxonResp.assessments || (taxonResp.taxon && taxonResp.taxon.assessments) || [];
  if (!assessments.length) {
    return { redListCategory: null, redListCategoryCode: null, populationTrend: null, assessmentYear: null, populationNarrative: null };
  }
  const latest = assessments.find(a => a.latest) || assessments[0];
  const assessmentId = latest.assessment_id || latest.sis_taxon_id || latest.id;

  const detail = await iucnFetch(`/assessment/${assessmentId}`);
  console.log(`[iucn] assessment detail for ${scientificName}:`, JSON.stringify(detail).slice(0, 400));

  const category = detail.red_list_category || {};
  const trend = detail.population_trend || detail.supplementary_info?.population_trend || {};
  const narrative = (detail.documentation && detail.documentation.population) ||
                     (detail.supplementary_info && detail.supplementary_info.population) || null;

  return {
    redListCategory: category.description?.en || category.code || null,
    redListCategoryCode: category.code || null,
    populationTrend: trend.description?.en || trend.code || null,
    assessmentYear: latest.year_published || null,
    populationNarrative: narrative,
  };
}

async function fetchAllPopulationStatus() {
  if (!IUCN_API_TOKEN) {
    console.log('[iucn] no IUCN_API_TOKEN configured -- skipping population-status lookup');
    return {};
  }
  const result = {};
  for (const species of SPECIES_LIST) {
    try {
      result[species] = await fetchIUCNStatus(species);
      console.log(`[iucn] ${species} -> category=${result[species].redListCategoryCode}, trend=${result[species].populationTrend}`);
    } catch (err) {
      console.error(`[iucn] failed for ${species}:`, err.message || err);
      result[species] = { redListCategory: null, redListCategoryCode: null, populationTrend: null, assessmentYear: null, populationNarrative: null };
    }
  }
  return result;
}

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
        // Optional Darwin Core fields -- only present when the source dataset
        // supplies them, so the dashboard shows them "when available".
        organismID: r.organismID || null,
        individualCount: r.individualCount || null,
        sex: r.sex || null,
        lifeStage: r.lifeStage || null,
        locality: r.locality || null,
        waterBody: r.waterBody || null,
        country: r.country || null,
        remarks: r.occurrenceRemarks || null,
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
// ---- World coastlines (static, bundled -- not a live fetch) ------------
// Uses the world-atlas npm package (real Natural Earth 1:110m land data,
// installed at build time) rather than fetching from a live URL. Land
// doesn't move day to day, so there's nothing to refresh, and it avoids
// depending on the uptime of yet another external service.
function buildLandOutlines() {
  const landObject = worldLandTopology.objects.land;
  const result = topojson.feature(worldLandTopology, landObject);
  const features = result.type === 'FeatureCollection' ? result.features : [result];
  const outlines = [];
  features.forEach(f => {
    const geom = f.geometry;
    if (!geom) return;
    if (geom.type === 'Polygon') {
      outlines.push(geom.coordinates[0]);
    } else if (geom.type === 'MultiPolygon') {
      geom.coordinates.forEach(poly => outlines.push(poly[0]));
    }
  });
  return outlines;
}

let LAND_OUTLINES = [];
let LAND_BUILT_AT = null;
try {
  LAND_OUTLINES = buildLandOutlines();
  LAND_BUILT_AT = new Date().toISOString();
  console.log(`[land] built ${LAND_OUTLINES.length} coastline shapes from world-atlas`);
} catch (err) {
  console.error('[land] failed to build coastline outlines:', err.message || err);
}

// ---- Geography: nearest town and distance from the coast ----------------
// Both are worked out here, once per refresh, so the page stays fast.
//  - Towns: GeoNames places with 10,000+ people (npm "all-the-cities"),
//    indexed into 2-degree cells.
//  - Coast: Natural Earth 1:50m coastline (world-atlas land-50m), indexed into
//    1-degree cells. 1:50m is detailed but still generalized, so distances
//    within a few km of shore are approximate.
const GEO_CITY_CELL = 2;
const GEO_COAST_CELL = 1;
const MIN_TOWN_POPULATION = 10000;
let cityIndex = null;
let coastIndex = null;
let landPolys50 = [];

function geoToRad(d){ return d * Math.PI / 180; }
function geoHaversineKm(lat1, lon1, lat2, lon2){
  const R = 6371;
  const dLat = geoToRad(lat2 - lat1), dLon = geoToRad(lon2 - lon1);
  const a = Math.sin(dLat/2)**2 + Math.cos(geoToRad(lat1))*Math.cos(geoToRad(lat2))*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.sqrt(a));
}
function geoCompass8(lat1, lon1, lat2, lon2){
  // direction FROM point 1 TO point 2
  const y = Math.sin(geoToRad(lon2-lon1)) * Math.cos(geoToRad(lat2));
  const x = Math.cos(geoToRad(lat1))*Math.sin(geoToRad(lat2)) - Math.sin(geoToRad(lat1))*Math.cos(geoToRad(lat2))*Math.cos(geoToRad(lon2-lon1));
  const deg = (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
  return ['N','NE','E','SE','S','SW','W','NW'][Math.round(deg / 45) % 8];
}
function wrapCellX(x, cellDeg){
  const n = 360 / cellDeg;
  return ((x + n/2) % n + n) % n - n/2;
}
// Visits grid cells in growing square rings around (lat, lon) until the best
// match found so far is closer than anything an outer ring could contain.
function searchRings(index, cellDeg, lat, lon, maxRings, visit){
  const cy = Math.floor(lat / cellDeg), cx = Math.floor(lon / cellDeg);
  const kmPerCell = cellDeg * 111.32 * Math.max(Math.cos(geoToRad(lat)), 0.2);
  let best = Infinity;
  for(let r = 0; r <= maxRings; r++){
    for(let y = cy - r; y <= cy + r; y++){
      for(let x = cx - r; x <= cx + r; x++){
        if(Math.max(Math.abs(y - cy), Math.abs(x - cx)) !== r) continue;
        const cell = index.get(y + ',' + wrapCellX(x, cellDeg));
        if(cell) best = Math.min(best, visit(cell));
      }
    }
    if(best < Infinity && best <= r * kmPerCell) break;
  }
  return best;
}

function buildCityIndex(cities){
  const index = new Map();
  let kept = 0;
  for(const c of cities){
    if(!c || !c.loc || !c.loc.coordinates || !(c.population >= MIN_TOWN_POPULATION)) continue;
    const lon = c.loc.coordinates[0], lat = c.loc.coordinates[1];
    const k = Math.floor(lat / GEO_CITY_CELL) + ',' + Math.floor(lon / GEO_CITY_CELL);
    if(!index.has(k)) index.set(k, []);
    index.get(k).push({ name: c.name, country: c.country, admin: c.adminCode, lat, lon });
    kept++;
  }
  return { index, kept };
}
function indexCoastSegments(lines){
  const index = new Map();
  let count = 0;
  for(const line of lines){
    for(let i = 1; i < line.length; i++){
      const a = line[i-1], b = line[i];
      if(Math.abs(a[0] - b[0]) > 180) continue; // skip date-line jumps
      const seg = [a[0], a[1], b[0], b[1]];
      const x0 = Math.floor(Math.min(a[0], b[0]) / GEO_COAST_CELL), x1 = Math.floor(Math.max(a[0], b[0]) / GEO_COAST_CELL);
      const y0 = Math.floor(Math.min(a[1], b[1]) / GEO_COAST_CELL), y1 = Math.floor(Math.max(a[1], b[1]) / GEO_COAST_CELL);
      for(let x = x0; x <= x1; x++) for(let y = y0; y <= y1; y++){
        const k = y + ',' + x;
        if(!index.has(k)) index.set(k, []);
        index.get(k).push(seg);
      }
      count++;
    }
  }
  return { index, count };
}
function segmentDistKm(lat, lon, s){
  const kx = 111.32 * Math.cos(geoToRad(lat)), ky = 110.57;
  const wrap = d => d > 180 ? d - 360 : (d < -180 ? d + 360 : d);
  const ax = wrap(s[0] - lon) * kx, ay = (s[1] - lat) * ky;
  const bx = wrap(s[2] - lon) * kx, by = (s[3] - lat) * ky;
  const dx = bx - ax, dy = by - ay, len2 = dx*dx + dy*dy;
  let t = len2 ? -(ax*dx + ay*dy) / len2 : 0;
  t = Math.max(0, Math.min(1, t));
  const px = ax + t*dx, py = ay + t*dy;
  return Math.sqrt(px*px + py*py);
}
function nearestCoastKm(lat, lon){
  if(!coastIndex) return null;
  const best = searchRings(coastIndex, GEO_COAST_CELL, lat, lon, 12, cell => {
    let m = Infinity;
    for(const s of cell){ const d = segmentDistKm(lat, lon, s); if(d < m) m = d; }
    return m;
  });
  return best === Infinity ? null : best;
}
function nearestTown(lat, lon){
  if(!cityIndex) return null;
  let bestTown = null;
  const best = searchRings(cityIndex, GEO_CITY_CELL, lat, lon, 15, cell => {
    let m = Infinity;
    for(const c of cell){
      const d = geoHaversineKm(lat, lon, c.lat, c.lon);
      if(d < m){ m = d; if(!bestTown || d < bestTown.d) bestTown = { c, d }; }
    }
    return m;
  });
  if(best === Infinity || !bestTown) return null;
  const c = bestTown.c;
  return {
    name: c.name, country: c.country, admin: c.admin,
    km: Math.round(bestTown.d * 10) / 10,
    direction: geoCompass8(c.lat, c.lon, lat, lon), // where the sighting is, seen from the town
  };
}
function pointInRingLonLat(lat, lon, ring){
  let inside = false;
  for(let i = 0, j = ring.length - 1; i < ring.length; j = i++){
    const xi = ring[i][0], yi = ring[i][1], xj = ring[j][0], yj = ring[j][1];
    if(((yi > lat) !== (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
  }
  return inside;
}
function insideMappedLand(lat, lon){
  for(const p of landPolys50){
    if(lon < p.bb[0] || lon > p.bb[1] || lat < p.bb[2] || lat > p.bb[3]) continue;
    if(pointInRingLonLat(lat, lon, p.ring)) return true;
  }
  return false;
}

try {
  const built = buildCityIndex(require('all-the-cities'));
  cityIndex = built.index;
  // free the full 138k-place list; only the 10,000+ towns are kept
  try { delete require.cache[require.resolve('all-the-cities')]; } catch (e) {}
  console.log(`[geo] town index: ${built.kept} places with ${MIN_TOWN_POPULATION.toLocaleString()}+ people`);
} catch (err) {
  console.error('[geo] town data unavailable (nearest-town lookups disabled):', err.message || err);
}
try {
  const topo50 = require('world-atlas/land-50m.json');
  const coast = topojson.mesh(topo50, topo50.objects.land);
  const built = indexCoastSegments(coast.type === 'MultiLineString' ? coast.coordinates : [coast.coordinates]);
  coastIndex = built.index;
  const landFc = topojson.feature(topo50, topo50.objects.land);
  const feats = landFc.type === 'FeatureCollection' ? landFc.features : [landFc];
  feats.forEach(f => {
    const g = f.geometry; if(!g) return;
    const polys = g.type === 'Polygon' ? [g.coordinates] : (g.type === 'MultiPolygon' ? g.coordinates : []);
    polys.forEach(poly => {
      const ring = poly[0];
      let a = 180, b = -180, c = 90, d = -90;
      for(const pt of ring){ if(pt[0]<a)a=pt[0]; if(pt[0]>b)b=pt[0]; if(pt[1]<c)c=pt[1]; if(pt[1]>d)d=pt[1]; }
      if(c < -85) return; // skip Antarctica's pole-wrapping ring; flat lon/lat tests misread it
      landPolys50.push({ ring, bb: [a, b, c, d] });
    });
  });
  console.log(`[geo] coastline index: ${built.count} segments, ${landPolys50.length} land shapes (Natural Earth 1:50m)`);
} catch (err) {
  console.error('[geo] 1:50m coastline unavailable (distance-from-coast disabled):', err.message || err);
}

// Adds nearestTown / coastKm / insideCoastline to each sighting in place.
function annotateGeography(occurrences){
  let towns = 0, coasts = 0;
  occurrences.forEach(o => {
    if(o.lat == null || o.lon == null) return;
    o.nearestTown = nearestTown(o.lat, o.lon);
    if(o.nearestTown) towns++;
    if(coastIndex){
      const km = nearestCoastKm(o.lat, o.lon);
      o.coastKm = km == null ? null : Math.round(km * 10) / 10;
      o.coastFar = km == null; // nothing within ~12 degrees
      o.insideCoastline = insideMappedLand(o.lat, o.lon);
      coasts++;
    }
  });
  console.log(`[geo] annotated ${occurrences.length} sightings (town: ${towns}, coast: ${coasts})`);
}

// ---- Generic Esri REST FeatureServer fetcher, with pagination ----------
// Esri services cap how many features one query returns (often 1000-2000).
// Loops with resultOffset until a page comes back smaller than requested,
// so a dataset bigger than one page's limit doesn't get silently truncated.
async function fetchEsriFeaturesAll(baseUrl, label, pageSize = 1000) {
  let allFeatures = [];
  let offset = 0;
  const sep = baseUrl.includes('?') ? '&' : '?';
  while (true) {
    const url = `${baseUrl}${sep}resultOffset=${offset}&resultRecordCount=${pageSize}`;
    console.log(`[${label}] requesting offset ${offset}:`, url);
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CetusBackend/0.1)' },
    });
    console.log(`[${label}] response status:`, res.status, res.statusText);
    if (!res.ok) {
      const bodyText = await res.text();
      console.log(`[${label}] error body:`, bodyText.slice(0, 500));
      throw new Error(`${label} request failed: ${res.status}`);
    }
    const geojson = await res.json();
    if (geojson.error) {
      console.error(`[${label}] Esri returned an error payload:`, JSON.stringify(geojson.error).slice(0, 500));
      throw new Error(`${label} API error: ${geojson.error.message || JSON.stringify(geojson.error)}`);
    }
    const features = geojson.features || [];
    if (offset === 0) {
      console.log(`[${label}] first feature geometry type:`, features[0] && features[0].geometry && features[0].geometry.type);
      console.log(`[${label}] first feature properties keys:`, Object.keys((features[0] && features[0].properties) || {}).slice(0, 15));
    }
    allFeatures = allFeatures.concat(features);
    if (features.length < pageSize) break; // last page
    offset += pageSize;
    if (offset > 20000) break; // safety cap against a runaway loop
  }
  console.log(`[${label}] total features across all pages: ${allFeatures.length}`);
  return allFeatures;
}

// Normalizes Esri GeoJSON features to our lightweight { id, ring, ...extra }
// shape -- one outer ring of [lon,lat] pairs per polygon part, dropping the
// many attribute fields we don't use.
function normalizePolygonFeatures(features, idFn, extraFn) {
  const polygons = [];
  features.forEach(f => {
    const geom = f.geometry;
    if (!geom) return;
    const props = f.properties || {};
    const id = idFn ? idFn(props) : null;
    const extra = extraFn ? extraFn(props) : {};
    if (geom.type === 'Polygon') {
      polygons.push({ id, ring: geom.coordinates[0], ...extra });
    } else if (geom.type === 'MultiPolygon') {
      geom.coordinates.forEach(poly => {
        polygons.push({ id, ring: poly[0], ...extra });
      });
    }
  });
  return polygons;
}

async function fetchDrillingLeases() {
  const features = await fetchEsriFeaturesAll(BOEM_LEASES_URL, 'leases');
  return normalizePolygonFeatures(features, props =>
    props.LEASE_NUMB || props.LEASE_NUMBER || props.LEASE_NO || null
  );
}

async function fetchRightWhaleSMA() {
  const features = await fetchEsriFeaturesAll(RIGHT_WHALE_SMA_URL, 'right-whale-sma');
  return normalizePolygonFeatures(
    features,
    props => props.zone_name || props.ZONE_NAME || null,
    props => ({
      restrictedPeriod: props.restrictedperiod || props.RESTRICTEDPERIOD || null,
    })
  );
}

async function fetchMPABoundaries() {
  const features = await fetchEsriFeaturesAll(MPA_BOUNDARIES_URL, 'mpa-boundaries');
  return normalizePolygonFeatures(
    features,
    props => props.Site_Name || props.SITE_NAME || props.NAME || null,
    props => ({
      protectionLevel: props.Prot_Lvl || null,
      governanceLevel: props.Gov_Level || null,
    })
  );
}

async function fetchMilitaryAreas() {
  const features = await fetchEsriFeaturesAll(MILITARY_OPAREA_URL, 'military-areas');
  return normalizePolygonFeatures(
    features,
    props => props.featurename || props.FEATURENAME || null,
    props => ({
      areaType: props.typedomain || null,
      installation: props.installationidentifier || null,
    })
  );
}

// ---- PADDDtracker.org -- bundled, static (not a live fetch) -----------
// Real, peer-reviewed research data on documented Protected Area
// Downgrading, Downsizing, and Degazettement events, filtered to the
// marine-zone subset. This is Australia's 2018 Commonwealth Marine
// Reserve rezoning specifically -- a real, well-documented case -- not a
// comprehensive global feed of every marine protection change everywhere.
// Bundled as a static file (manually downloaded + converted from the
// official Zenodo shapefile release) rather than fetched live, since
// this is a fixed historical dataset, not something that changes daily.
//
// Required citation for this data (keep this attached wherever it's shown):
// Conservation International, & World Wildlife Fund. (2021).
// PADDDtracker Data Release Version 2.1. Zenodo.
// https://doi.org/10.5281/zenodo.4974336
const PADDD_CITATION = 'Conservation International & World Wildlife Fund (2021). PADDDtracker Data Release Version 2.1. Zenodo. https://doi.org/10.5281/zenodo.4974336';
let PADDD_EVENTS = [];
try {
  const raw = fs.readFileSync(path.join(__dirname, 'data', 'paddd-mpa-events.json'), 'utf8');
  PADDD_EVENTS = JSON.parse(raw);
  console.log(`[paddd] loaded ${PADDD_EVENTS.length} documented protection-change events (bundled, static)`);
} catch (err) {
  console.error('[paddd] failed to load bundled PADDD dataset:', err.message || err);
}

// ---- refresh cycle ------------------------------------------------------
async function refreshAll() {
  try {
    const fresh = await fetchWhaleOccurrences();
    try { annotateGeography(fresh); } catch (geoErr) { console.error('[geo] annotate failed:', geoErr.message || geoErr); }
    cache.whaleOccurrences.data = fresh;
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

  try {
    cache.rightWhaleSMA.data = await fetchRightWhaleSMA();
    cache.rightWhaleSMA.updatedAt = new Date().toISOString();
    console.log(`[refresh] Right whale SMA: ${cache.rightWhaleSMA.data.length} polygons`);
  } catch (err) {
    console.error('[refresh] Right whale SMA fetch failed:', err.message || err);
  }

  try {
    cache.mpaBoundaries.data = await fetchMPABoundaries();
    cache.mpaBoundaries.updatedAt = new Date().toISOString();
    console.log(`[refresh] MPA boundaries: ${cache.mpaBoundaries.data.length} polygons`);
  } catch (err) {
    console.error('[refresh] MPA boundaries fetch failed:', err.message || err);
  }

  try {
    cache.militaryAreas.data = await fetchMilitaryAreas();
    cache.militaryAreas.updatedAt = new Date().toISOString();
    console.log(`[refresh] Military operating areas: ${cache.militaryAreas.data.length} polygons`);
  } catch (err) {
    console.error('[refresh] Military operating areas fetch failed:', err.message || err);
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

app.get('/api/right-whale-sma', (req, res) => {
  res.json(cache.rightWhaleSMA);
});

app.get('/api/military-areas', (req, res) => {
  res.json(cache.militaryAreas);
});

app.get('/api/mpa-boundaries', (req, res) => {
  res.json(cache.mpaBoundaries);
});

app.get('/api/paddd', (req, res) => {
  res.json({
    citation: PADDD_CITATION,
    scope: "Australia's 2018 Commonwealth Marine Reserve rezoning specifically -- not a comprehensive global feed of every marine protection change.",
    count: PADDD_EVENTS.length,
    data: PADDD_EVENTS,
  });
});

app.get('/api/species-images', (req, res) => {
  res.json(cache.speciesImages);
});

app.get('/api/population-status', (req, res) => {
  res.json({
    ...cache.populationStatus,
    configured: !!IUCN_API_TOKEN,
    citation: 'IUCN 2025. The IUCN Red List of Threatened Species. https://www.iucnredlist.org',
  });
});

app.get('/api/land', (req, res) => {
  res.json({
    updatedAt: LAND_BUILT_AT,
    data: LAND_OUTLINES.map(ring => ({ ring })),
  });
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
    rightWhaleSMA: {
      updatedAt: cache.rightWhaleSMA.updatedAt,
      count: cache.rightWhaleSMA.data.length,
      scope: 'NOAA US Atlantic seaboard speed-restriction zones only',
    },
    mpaBoundaries: {
      updatedAt: cache.mpaBoundaries.updatedAt,
      count: cache.mpaBoundaries.data.length,
      scope: 'NOAA US Marine Protected Areas Inventory — US only',
    },
    militaryAreas: {
      updatedAt: cache.militaryAreas.updatedAt,
      count: cache.militaryAreas.data.length,
      scope: 'NOAA/MarineCadastre military Operating Areas — US waters, Hawaii, Guam; boundaries only, no exercise schedules',
    },
    paddd: {
      count: PADDD_EVENTS.length,
      scope: "Bundled, static — Australia's 2018 Commonwealth Marine Reserve rezoning specifically",
    },
    landOutlines: {
      builtAt: LAND_BUILT_AT,
      count: LAND_OUTLINES.length,
    },
    note: 'No continuous single-whale tracks here — that needs an Argos data-sharing agreement.',
  });
});

app.use(express.static('../Public'));

app.listen(PORT, async () => {
  console.log(`Cetus backend listening on :${PORT}`);
  await refreshAll(); // populate cache on boot

  // Species images are essentially static -- fetch once at startup rather
  // than on every refresh cycle, to avoid hammering Wikipedia needlessly.
  try {
    cache.speciesImages.data = await fetchAllSpeciesImages();
    cache.speciesImages.updatedAt = new Date().toISOString();
  } catch (err) {
    console.error('[species-image] batch fetch failed:', err.message || err);
  }

  // Same idea for IUCN conservation status -- a species' Red List category
  // and population trend don't change day to day, so fetch once at startup.
  try {
    cache.populationStatus.data = await fetchAllPopulationStatus();
    cache.populationStatus.updatedAt = new Date().toISOString();
  } catch (err) {
    console.error('[iucn] batch fetch failed:', err.message || err);
  }

  cron.schedule(`*/${REFRESH_MINUTES} * * * *`, refreshAll);
});
