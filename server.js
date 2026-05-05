import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 8082;
const DATA_DIR = path.join(__dirname, '.data');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const REPORTS_FILE = path.join(DATA_DIR, 'reports.json');

fs.mkdirSync(DATA_DIR, { recursive: true });
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function saveJson(file, value) {
  fs.writeFileSync(file, JSON.stringify(value, null, 2));
}
function getConfig() { return loadJson(CONFIG_FILE, null); }
function setConfig(cfg) { saveJson(CONFIG_FILE, cfg); }
function getReports() { return loadJson(REPORTS_FILE, []); }
function setReports(reports) { saveJson(REPORTS_FILE, reports); }
function authHeader(cfg) {
  return 'Basic ' + Buffer.from(`${cfg.jiraUser}:${cfg.jiraToken}`).toString('base64');
}
function normalizeBaseUrl(url) {
  const raw = String(url || '').trim().replace(/\/$/, '');
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://${raw}`;
}
async function jiraFetch(cfg, jiraPath, options = {}) {
  const base = normalizeBaseUrl(cfg.jiraBaseUrl);
  if (!base) throw new Error('Missing Jira base URL');
  const url = `${base}/rest/api/${cfg.jiraApiVersion || '3'}${jiraPath}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Accept: 'application/json',
      Authorization: authHeader(cfg),
      ...(options.headers || {})
    }
  });
  const text = await res.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }
  if (!res.ok) {
    const msg = body && typeof body === 'object' ? (body.errorMessages?.join(', ') || body.message || text) : text;
    const err = new Error(`Jira ${res.status}: ${msg}`);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return body;
}

app.get('/health', (_req, res) => res.json({ ok: true }));

app.get('/api/config', (_req, res) => res.json(getConfig() || {}));
app.post('/api/config', (req, res) => {
  const cfg = req.body || {};
  setConfig(cfg);
  res.json({ ok: true });
});

app.post('/api/config/test', async (req, res) => {
  const cfg = req.body || getConfig();
  if (!cfg?.jiraBaseUrl || !cfg?.jiraUser || !cfg?.jiraToken) {
    return res.status(400).json({ ok: false, error: 'Missing Jira config' });
  }
  try {
    const myself = await jiraFetch(cfg, '/myself');
    res.json({ ok: true, myself });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: err.message, body: err.body });
  }
});

app.get('/api/reports', (_req, res) => res.json(getReports()));
app.post('/api/reports', (req, res) => {
  const reports = getReports();
  const report = req.body || {};
  if (!report.title || !report.jql) return res.status(400).json({ ok: false, error: 'title and jql required' });
  const next = { id: report.id || crypto.randomUUID(), title: report.title, group: report.group || '', jql: report.jql };
  reports.push(next);
  setReports(reports);
  res.json(next);
});
app.put('/api/reports/:id', (req, res) => {
  const reports = getReports();
  const idx = reports.findIndex(r => r.id === req.params.id);
  if (idx < 0) return res.status(404).json({ ok: false, error: 'not found' });
  reports[idx] = { ...reports[idx], ...req.body, id: reports[idx].id };
  setReports(reports);
  res.json(reports[idx]);
});
app.delete('/api/reports/:id', (req, res) => {
  const reports = getReports().filter(r => r.id !== req.params.id);
  setReports(reports);
  res.json({ ok: true });
});

function buildJiraSearchRequest(cfg, report) {
  const base = normalizeBaseUrl(cfg.jiraBaseUrl);
  const url = `${base}/rest/api/3/search/jql`;
  const body = {
    jql: report.jql,
    maxResults: Number(cfg.jiraPageSize || 50),
    fields: ['summary', 'status', 'assignee', 'updated', 'priority', 'issuetype']
  };
  return { url, body };
}

app.post('/api/reports/test', (req, res) => {
  const cfg = req.body?.config || getConfig();
  const reportIds = Array.isArray(req.body?.reportIds) ? req.body.reportIds : [];
  const allReports = getReports();
  const reports = allReports.filter(r => reportIds.includes(r.id));
  if (!reports.length) {
    return res.status(400).json({
      ok: false,
      error: 'No reports selected',
      debug: { reportIds, availableReportIds: allReports.map(r => r.id) }
    });
  }
  const preview = reports.map(report => ({ id: report.id, title: report.title, jql: report.jql, request: buildJiraSearchRequest(cfg, report) }));
  res.json({ ok: true, preview });
});

app.post('/api/reports/run', async (req, res) => {
  const cfg = req.body?.config || getConfig();
  const reportIds = Array.isArray(req.body?.reportIds) ? req.body.reportIds : [];
  const allReports = getReports();
  const reports = allReports.filter(r => reportIds.includes(r.id));
  if (!reports.length) {
    return res.status(400).json({
      ok: false,
      error: 'No reports selected',
      debug: { reportIds, availableReportIds: allReports.map(r => r.id) }
    });
  }
  try {
    const results = [];
    for (const report of reports) {
      const request = buildJiraSearchRequest(cfg, report);
      const res = await fetch(request.url, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          Authorization: authHeader(cfg)
        },
        body: JSON.stringify(request.body)
      });
      const text = await res.text();
      let search = null;
      try { search = text ? JSON.parse(text) : null; } catch { search = text; }
      if (!res.ok) {
        const msg = search && typeof search === 'object' ? (search.errorMessages?.join(', ') || search.message || text) : text;
        throw Object.assign(new Error(`Jira ${res.status}: ${msg}`), { status: res.status, body: search });
      }
      results.push({
        id: report.id,
        title: report.title,
        jql: report.jql,
        count: search.total || 0,
        issues: (search.issues || []).map(x => ({
          key: x.key,
          summary: x.fields?.summary || '',
          status: x.fields?.status?.name || '',
          assignee: x.fields?.assignee?.displayName || x.fields?.assignee?.emailAddress || '',
          updated: x.fields?.updated || '',
          priority: x.fields?.priority?.name || '',
          issuetype: x.fields?.issuetype?.name || ''
        }))
      });
    }
    res.json({ ok: true, results });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: err.message, body: err.body, details: err.body || null });
  }
});

app.listen(PORT, () => {
  console.log(`Jira Report Studio running on http://localhost:${PORT}`);
});
