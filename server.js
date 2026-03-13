const express = require('express');
const fs = require('fs');
const fsPromises = require('fs/promises');
const multer = require('multer');
const nodemailer = require('nodemailer');
const crypto = require('crypto');
const { parse } = require('csv-parse/sync');
const path = require('path');

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }
});

const DATA_DIR = path.join(__dirname, 'data');
const TEMP_UPLOADS_DIR = path.join(DATA_DIR, 'uploads');
const HISTORY_FILES_DIR = path.join(DATA_DIR, 'history-files');
const HISTORY_JSON_PATH = path.join(DATA_DIR, 'email-history.json');
const HISTORY_LIMIT_RAW = Number.parseInt(process.env.HISTORY_LIMIT || '0', 10);
const HISTORY_LIMIT =
  Number.isFinite(HISTORY_LIMIT_RAW) && HISTORY_LIMIT_RAW > 0 ? HISTORY_LIMIT_RAW : 0;
const SENT_EMAILS_DEFAULT_LIMIT = 25;
const SENT_EMAILS_MAX_LIMIT = 100;
const MAX_SUCCESSFUL_TOUCHES = 3;
const RESULT_EXPORT_COLUMNS = [
  'send_status',
  'send_error',
  'sent_at',
  'recipient_email',
  'provider_used',
  'subject_used',
  'message_id'
];
let sentEmailIndexCache = null;

app.use(express.json({ limit: '20mb' }));
app.use(express.static(path.join(__dirname, 'public')));

async function ensureDataStorage() {
  await fsPromises.mkdir(TEMP_UPLOADS_DIR, { recursive: true });
  await fsPromises.mkdir(HISTORY_FILES_DIR, { recursive: true });
}

function createId(prefix = '') {
  return `${prefix}${Date.now().toString(36)}${crypto.randomBytes(6).toString('hex')}`;
}

function sanitizeFilename(value, fallback = 'file.csv') {
  const cleaned = String(value || fallback)
    .trim()
    .replace(/[^\w.-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');

  return cleaned || fallback;
}

async function saveUploadedCsvFile(file) {
  await ensureDataStorage();

  const uploadId = createId('upload_');
  const originalName = String(file?.originalname || 'leads.csv').trim() || 'leads.csv';
  const storedFilename = `${uploadId}-${sanitizeFilename(originalName, 'leads.csv')}`;
  const storedPath = path.join(TEMP_UPLOADS_DIR, storedFilename);
  const sizeBytes = Number(file?.size || file?.buffer?.length || 0);

  await fsPromises.writeFile(storedPath, file.buffer);
  await fsPromises.writeFile(
    path.join(TEMP_UPLOADS_DIR, `${uploadId}.json`),
    JSON.stringify({ originalName, sizeBytes }, null, 2)
  );

  return {
    id: uploadId,
    name: originalName,
    sizeBytes
  };
}

async function findUploadedCsvFile(uploadId) {
  if (!uploadId) return null;

  await ensureDataStorage();
  const entries = await fsPromises.readdir(TEMP_UPLOADS_DIR, { withFileTypes: true });
  const match = entries.find(
    (entry) => entry.isFile() && entry.name.startsWith(`${String(uploadId).trim()}-`)
  );

  if (!match) return null;

  const filePath = path.join(TEMP_UPLOADS_DIR, match.name);
  const stats = await fsPromises.stat(filePath);
  let originalName = match.name.replace(`${uploadId}-`, '') || 'leads.csv';

  try {
    const metaRaw = await fsPromises.readFile(path.join(TEMP_UPLOADS_DIR, `${uploadId}.json`), 'utf8');
    const meta = JSON.parse(metaRaw);
    if (meta?.originalName) {
      originalName = String(meta.originalName);
    }
  } catch (_error) {
    // fall back to stored file name
  }

  return {
    filePath,
    originalName,
    sizeBytes: stats.size
  };
}

async function readHistoryEntries() {
  await ensureDataStorage();

  try {
    const raw = await fsPromises.readFile(HISTORY_JSON_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }

    throw error;
  }
}

function invalidateSentEmailIndexCache() {
  sentEmailIndexCache = null;
}

async function writeHistoryEntries(entries) {
  await ensureDataStorage();
  await fsPromises.writeFile(HISTORY_JSON_PATH, JSON.stringify(entries, null, 2));
  invalidateSentEmailIndexCache();
}

function toCsvCell(value) {
  const raw = value == null ? '' : String(value);
  const escaped = raw.replace(/"/g, '""');
  return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
}

function summarizeText(value, limit = 280) {
  return String(value || '').slice(0, limit);
}

function sanitizeLeadData(lead = {}) {
  const cleaned = {};

  for (const [key, value] of Object.entries(lead || {})) {
    const normalizedKey = String(key || '').trim();
    if (!normalizedKey) continue;
    cleaned[normalizedKey] = value == null ? '' : String(value);
  }

  return cleaned;
}

function createStableId(prefix, parts = []) {
  const hash = crypto.createHash('sha1').update(parts.map((part) => String(part || '')).join('|')).digest('hex');
  return `${prefix}${hash.slice(0, 20)}`;
}

function collectColumns(rows = []) {
  const columns = [];
  const seen = new Set();

  rows.forEach((row) => {
    Object.keys(row || {}).forEach((key) => {
      if (!seen.has(key)) {
        seen.add(key);
        columns.push(key);
      }
    });
  });

  return columns;
}

function clampNumber(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function parseNonNegativeInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }

  return parsed;
}

function extractLeadFieldValue(leadData = {}, candidates = []) {
  const entries = Object.entries(leadData || {});

  for (const candidate of candidates) {
    const match = entries.find(([key]) => normalizeKey(key) === normalizeKey(candidate));
    const value = match ? String(match[1] || '').trim() : '';
    if (value) return value;
  }

  return '';
}

function buildLeadPreviewItems(leadData = {}, limit = 4) {
  return Object.entries(leadData || {})
    .filter(([, value]) => String(value || '').trim())
    .slice(0, limit)
    .map(([key, value]) => ({
      key: String(key || ''),
      value: String(value || '')
    }));
}

function ageDaysSinceIso(value) {
  const timestamp = new Date(value || 0).getTime();
  if (!Number.isFinite(timestamp) || !timestamp) return 0;
  return (Date.now() - timestamp) / (1000 * 60 * 60 * 24);
}

function isThreadEligibleForFollowUp(thread, minimumAgeDays = 0) {
  if (!thread?.latestSuccessfulRecord) return false;
  if (Number(thread.totalSuccessfulSends || 0) >= MAX_SUCCESSFUL_TOUCHES) return false;
  return ageDaysSinceIso(thread.lastAttemptAt || thread.lastSentAt) >= Number(minimumAgeDays || 0);
}

function buildResultsExportCsv(columns = [], leads = [], results = []) {
  const baseColumns =
    Array.isArray(columns) && columns.length ? columns : Object.keys(leads[0] || {});
  const resultMap = new Map();
  results.forEach((result) => resultMap.set(result.rowNumber, result));

  const headers = [...baseColumns, ...RESULT_EXPORT_COLUMNS];

  const lines = [headers.map(toCsvCell).join(',')];

  leads.forEach((lead, index) => {
    const rowNumber = index + 1;
    const result = resultMap.get(rowNumber) || {};

    const merged = {
      ...lead,
      send_status: result.status || 'not_attempted',
      send_error: result.error || '',
      sent_at: result.sentAt || '',
      recipient_email: result.to || '',
      provider_used: result.provider || '',
      subject_used: result.subject || '',
      message_id: result.messageId || ''
    };

    lines.push(headers.map((header) => toCsvCell(merged[header])).join(','));
  });

  return lines.join('\n');
}

async function copySourceFileToHistory(entryId, sourceFileId) {
  const uploadedFile = await findUploadedCsvFile(sourceFileId);
  if (!uploadedFile) return null;

  const downloadName = uploadedFile.originalName || 'leads.csv';
  const storedFilename = `${entryId}-source-${sanitizeFilename(downloadName, 'leads.csv')}`;
  const targetPath = path.join(HISTORY_FILES_DIR, storedFilename);

  await fsPromises.copyFile(uploadedFile.filePath, targetPath);

  return {
    name: downloadName,
    storedFilename,
    sizeBytes: uploadedFile.sizeBytes
  };
}

async function writeResultsFileToHistory(entryId, columns, leads, results, sourceFileName = '') {
  const csvText = buildResultsExportCsv(columns, leads, results);
  const baseName = String(sourceFileName || 'campaign-results').replace(/\.csv$/i, '');
  const downloadName = `${baseName || 'campaign-results'}-results.csv`;
  const storedFilename = `${entryId}-results-${sanitizeFilename(downloadName, 'campaign-results.csv')}`;
  const targetPath = path.join(HISTORY_FILES_DIR, storedFilename);

  await fsPromises.writeFile(targetPath, csvText, 'utf8');

  return {
    name: downloadName,
    storedFilename,
    sizeBytes: Buffer.byteLength(csvText, 'utf8')
  };
}

async function writeRecipientRecordsFileToHistory(entryId, records = []) {
  const text = JSON.stringify(records, null, 2);
  const storedFilename = `${entryId}-recipient-records.json`;
  const targetPath = path.join(HISTORY_FILES_DIR, storedFilename);

  await fsPromises.writeFile(targetPath, text, 'utf8');

  return {
    storedFilename,
    sizeBytes: Buffer.byteLength(text, 'utf8'),
    recordCount: Array.isArray(records) ? records.length : 0
  };
}

async function readHistoryJsonFile(storedFilename) {
  if (!storedFilename) return null;

  const filePath = path.join(HISTORY_FILES_DIR, storedFilename);
  if (!fs.existsSync(filePath)) return null;

  const raw = await fsPromises.readFile(filePath, 'utf8');
  return raw ? JSON.parse(raw) : null;
}

function buildRecipientRecord(input = {}) {
  const id = String(input.id || createId('msg_'));
  const historyEntryId = String(input.historyEntryId || '');
  const rootHistoryEntryId = String(input.rootHistoryEntryId || historyEntryId);
  const threadId = String(input.threadId || id);
  const rowNumber = Number(input.rowNumber);
  const touchNumber = Math.max(1, Number(input.touchNumber || 1));

  return {
    id,
    threadId,
    parentRecordId: input.parentRecordId ? String(input.parentRecordId) : '',
    historyEntryId,
    rootHistoryEntryId,
    campaignType: String(input.campaignType || 'bulk'),
    rowNumber: Number.isFinite(rowNumber) && rowNumber > 0 ? rowNumber : 0,
    to: String(input.to || '').trim(),
    status: String(input.status || ''),
    error: String(input.error || ''),
    subject: String(input.subject || ''),
    bodyText: String(input.bodyText || ''),
    bodyPreview: summarizeText(input.bodyText || input.bodyPreview || ''),
    sentAt: String(input.sentAt || new Date().toISOString()),
    messageId: String(input.messageId || ''),
    providerType: String(input.providerType || ''),
    providerName: String(input.providerName || ''),
    leadData: sanitizeLeadData(input.leadData),
    touchNumber,
    followUpNumber: Math.max(0, touchNumber - 1),
    campaignCreatedAt: String(input.campaignCreatedAt || ''),
    legacy: Boolean(input.legacy)
  };
}

function stripResultExportColumns(row = {}) {
  return Object.fromEntries(
    Object.entries(row).filter(([key]) => !RESULT_EXPORT_COLUMNS.includes(String(key || '').trim()))
  );
}

function buildLegacyPreviewRecipientRecords(entry = {}) {
  const previewResults = Array.isArray(entry.previewResults) ? entry.previewResults : [];

  return previewResults.map((row, index) => {
    const stableId = createStableId('legacy_', [
      entry.id,
      row.rowNumber || index + 1,
      row.to || entry.recipient || '',
      row.sentAt || entry.createdAt || '',
      row.subject || entry.subject || ''
    ]);

    return buildRecipientRecord({
      id: stableId,
      threadId: stableId,
      historyEntryId: entry.id,
      rootHistoryEntryId: entry.id,
      campaignType: entry.type || 'test',
      rowNumber: Number(row.rowNumber || index + 1),
      to: row.to || entry.recipient || '',
      status: row.status || 'sent',
      error: row.error || '',
      subject: row.subject || entry.subject || entry.subjectTemplate || '',
      bodyText: '',
      bodyPreview: entry.bodyPreview || '',
      sentAt: row.sentAt || entry.createdAt || '',
      messageId: row.messageId || '',
      providerType: entry.providerType || '',
      providerName: entry.providerName || '',
      touchNumber: Number(row.touchNumber || 1),
      campaignCreatedAt: entry.createdAt || '',
      legacy: true
    });
  });
}

function buildLegacyResultsRecipientRecords(entry = {}, parsedRows = []) {
  return parsedRows.map((row, index) => {
    const rowNumber = index + 1;
    const recipient = String(row.recipient_email || resolveField(row, entry.emailColumn) || '').trim();
    const stableId = createStableId('legacy_', [
      entry.id,
      rowNumber,
      recipient,
      row.sent_at || entry.createdAt || '',
      row.subject_used || entry.subject || entry.subjectTemplate || ''
    ]);

    return buildRecipientRecord({
      id: stableId,
      threadId: stableId,
      historyEntryId: entry.id,
      rootHistoryEntryId: entry.id,
      campaignType: entry.type || 'bulk',
      rowNumber,
      to: recipient,
      status: row.send_status || 'unknown',
      error: row.send_error || '',
      subject: row.subject_used || entry.subject || entry.subjectTemplate || '',
      bodyText: '',
      bodyPreview: entry.bodyPreview || '',
      sentAt: row.sent_at || entry.createdAt || '',
      messageId: row.message_id || '',
      providerType: row.provider_used || entry.providerType || '',
      providerName: entry.providerName || '',
      leadData: stripResultExportColumns(row),
      touchNumber: 1,
      campaignCreatedAt: entry.createdAt || '',
      legacy: true
    });
  });
}

async function loadEntryRecipientRecords(entry = {}) {
  if (entry.recipientsFile?.storedFilename) {
    try {
      const stored = await readHistoryJsonFile(entry.recipientsFile.storedFilename);
      const rows = Array.isArray(stored) ? stored : [];
      return rows.map((row, index) =>
        buildRecipientRecord({
          ...row,
          id:
            row?.id ||
            createStableId('legacy_', [entry.id, row?.to || '', row?.sentAt || entry.createdAt || '', index]),
          historyEntryId: row?.historyEntryId || entry.id,
          rootHistoryEntryId: row?.rootHistoryEntryId || entry.id,
          campaignType: row?.campaignType || entry.type || 'bulk',
          providerType: row?.providerType || entry.providerType || '',
          providerName: row?.providerName || entry.providerName || '',
          campaignCreatedAt: row?.campaignCreatedAt || entry.createdAt || ''
        })
      );
    } catch (_error) {
      // Fall through to legacy exports if the dedicated recipient file is missing or malformed.
    }
  }

  if (entry.resultsFile?.storedFilename) {
    try {
      const filePath = path.join(HISTORY_FILES_DIR, entry.resultsFile.storedFilename);
      if (!fs.existsSync(filePath)) {
        return buildLegacyPreviewRecipientRecords(entry);
      }

      const raw = await fsPromises.readFile(filePath, 'utf8');
      const rows = parse(raw, {
        bom: true,
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        trim: true
      });

      return buildLegacyResultsRecipientRecords(entry, rows);
    } catch (_error) {
      return buildLegacyPreviewRecipientRecords(entry);
    }
  }

  return buildLegacyPreviewRecipientRecords(entry);
}

async function loadAllRecipientRecords(entries = []) {
  const nested = await Promise.all(entries.map((entry) => loadEntryRecipientRecords(entry)));
  return nested
    .flat()
    .sort((left, right) => new Date(right.sentAt || 0).getTime() - new Date(left.sentAt || 0).getTime());
}

function buildSentEmailThreads(records = []) {
  const groups = new Map();

  records.forEach((record) => {
    const key = record.threadId || record.id;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(record);
  });

  return Array.from(groups.entries())
    .map(([threadId, rows]) => {
      const timeline = rows.sort(
        (left, right) => new Date(right.sentAt || 0).getTime() - new Date(left.sentAt || 0).getTime()
      );
      const latestAttempt = timeline[0] || null;
      const latestSuccessful = timeline.find((item) => item.status === 'sent') || null;
      const firstSuccessful = [...timeline].reverse().find((item) => item.status === 'sent') || latestSuccessful;
      const recipient = latestSuccessful?.to || latestAttempt?.to || '';
      const leadData = latestSuccessful?.leadData || latestAttempt?.leadData || {};
      const contactName = extractLeadFieldValue(leadData, [
        'first_name',
        'firstname',
        'first name',
        'name',
        'full_name',
        'full name'
      ]);
      const company = extractLeadFieldValue(leadData, [
        'company',
        'organization',
        'business',
        'account',
        'company_name',
        'company name'
      ]);
      const totalSuccessfulSends = timeline.filter((item) => item.status === 'sent').length;
      const totalFailedAttempts = timeline.filter((item) => item.status !== 'sent').length;
      const searchText = [
        recipient,
        latestSuccessful?.subject || latestAttempt?.subject || '',
        contactName,
        company,
        ...Object.values(leadData || {})
      ]
        .join(' ')
        .toLowerCase();

      return {
        id: threadId,
        recipient,
        latestRecordId: latestAttempt?.id || '',
        latestSuccessfulRecordId: latestSuccessful?.id || '',
        status: latestAttempt?.status || 'unknown',
        lastAttemptAt: latestAttempt?.sentAt || '',
        lastSentAt: latestSuccessful?.sentAt || latestAttempt?.sentAt || '',
        lastSubject: latestSuccessful?.subject || latestAttempt?.subject || '',
        lastBodyPreview: latestSuccessful?.bodyPreview || latestAttempt?.bodyPreview || '',
        providerType: latestAttempt?.providerType || '',
        providerName: latestAttempt?.providerName || '',
        totalAttempts: timeline.length,
        totalSuccessfulSends,
        totalFailedAttempts,
        followUpCount: Math.max(0, Number(latestSuccessful?.followUpNumber || 0)),
        canFollowUp: Boolean(latestSuccessful) && totalSuccessfulSends < MAX_SUCCESSFUL_TOUCHES,
        rootHistoryEntryId:
          firstSuccessful?.rootHistoryEntryId ||
          latestSuccessful?.rootHistoryEntryId ||
          latestAttempt?.rootHistoryEntryId ||
          '',
        contactName,
        company,
        leadPreview: buildLeadPreviewItems(leadData),
        leadData,
        searchText,
        latestAttemptRecord: latestAttempt,
        latestSuccessfulRecord: latestSuccessful,
        timeline
      };
    })
    .sort((left, right) => new Date(right.lastAttemptAt || 0).getTime() - new Date(left.lastAttemptAt || 0).getTime());
}

function buildSentEmailStats(records = [], threads = []) {
  return {
    totalRecords: records.length,
    totalSent: records.filter((item) => item.status === 'sent').length,
    totalFailed: records.filter((item) => item.status !== 'sent').length,
    totalThreads: threads.length,
    followUpReady: threads.filter((thread) => isThreadEligibleForFollowUp(thread, 0)).length
  };
}

function filterSentEmailThreads(threads = [], options = {}) {
  const query = String(options.query || '')
    .trim()
    .toLowerCase();
  const status = String(options.status || 'all').trim().toLowerCase();
  const minAgeDays = Number(options.minAgeDays || 0);

  return threads.filter((thread) => {
    if (minAgeDays > 0 && ageDaysSinceIso(thread.lastAttemptAt || thread.lastSentAt) < minAgeDays) {
      return false;
    }

    if (query && !String(thread.searchText || '').includes(query)) {
      return false;
    }

    if (status === 'ready') {
      return isThreadEligibleForFollowUp(thread, minAgeDays);
    }

    if (status === 'sent') {
      return Number(thread.totalSuccessfulSends || 0) > 0;
    }

    if (status === 'failed') {
      return thread.status !== 'sent';
    }

    if (status === 'first-follow-up') {
      return Boolean(thread.latestSuccessfulRecordId) && Number(thread.totalSuccessfulSends || 0) === 1;
    }

    return true;
  });
}

function buildSentEmailThreadListItem(thread = {}) {
  return {
    id: String(thread.id || ''),
    recipient: String(thread.recipient || ''),
    latestRecordId: String(thread.latestRecordId || ''),
    latestSuccessfulRecordId: String(thread.latestSuccessfulRecordId || ''),
    status: String(thread.status || 'unknown'),
    lastAttemptAt: String(thread.lastAttemptAt || ''),
    lastSentAt: String(thread.lastSentAt || ''),
    lastSubject: String(thread.lastSubject || ''),
    lastBodyPreview: String(thread.lastBodyPreview || ''),
    providerType: String(thread.providerType || ''),
    providerName: String(thread.providerName || ''),
    totalAttempts: Number(thread.totalAttempts || 0),
    totalSuccessfulSends: Number(thread.totalSuccessfulSends || 0),
    totalFailedAttempts: Number(thread.totalFailedAttempts || 0),
    followUpCount: Number(thread.followUpCount || 0),
    canFollowUp: Boolean(thread.canFollowUp),
    rootHistoryEntryId: String(thread.rootHistoryEntryId || ''),
    contactName: String(thread.contactName || ''),
    company: String(thread.company || ''),
    leadPreview: Array.isArray(thread.leadPreview) ? thread.leadPreview : []
  };
}

function buildSentEmailThreadDetail(thread = {}) {
  const summary = buildSentEmailThreadListItem(thread);

  return {
    ...summary,
    leadData: sanitizeLeadData(thread.leadData),
    timeline: Array.isArray(thread.timeline)
      ? thread.timeline.map((event) => ({
          id: String(event.id || ''),
          parentRecordId: String(event.parentRecordId || ''),
          campaignType: String(event.campaignType || ''),
          to: String(event.to || ''),
          status: String(event.status || ''),
          error: String(event.error || ''),
          subject: String(event.subject || ''),
          bodyPreview: String(event.bodyPreview || ''),
          sentAt: String(event.sentAt || ''),
          messageId: String(event.messageId || ''),
          providerType: String(event.providerType || ''),
          providerName: String(event.providerName || ''),
          touchNumber: Number(event.touchNumber || 0),
          followUpNumber: Number(event.followUpNumber || 0),
          historyEntryId: String(event.historyEntryId || ''),
          rootHistoryEntryId: String(event.rootHistoryEntryId || '')
        }))
      : []
  };
}

async function loadSentEmailIndex() {
  const entries = await readHistoryEntries();
  const signature = JSON.stringify(
    entries.map((entry) => ({
      id: entry.id,
      createdAt: entry.createdAt,
      recipientsFile: entry.recipientsFile?.storedFilename || '',
      resultsFile: entry.resultsFile?.storedFilename || '',
      summary: entry.summary || {}
    }))
  );

  if (sentEmailIndexCache?.signature === signature) {
    return sentEmailIndexCache.data;
  }

  const records = await loadAllRecipientRecords(entries);
  const threads = buildSentEmailThreads(records);
  const data = {
    records,
    threads,
    stats: buildSentEmailStats(records, threads)
  };

  sentEmailIndexCache = {
    signature,
    data
  };

  return data;
}

function buildFollowUpContext(record = {}) {
  const leadData = sanitizeLeadData(record.leadData);
  const touchNumber = Number(record.touchNumber || 1);

  return {
    ...leadData,
    recipient_email: String(record.to || ''),
    previous_subject: String(record.subject || ''),
    previous_body: String(record.bodyText || record.bodyPreview || ''),
    previous_sent_at: String(record.sentAt || ''),
    touch_number: String(touchNumber + 1),
    follow_up_number: String(Math.max(1, touchNumber)),
    prior_touch_number: String(touchNumber)
  };
}

function buildFollowUpExportLead(record = {}, index = 0) {
  return {
    ...sanitizeLeadData(record.leadData),
    original_recipient: String(record.to || ''),
    original_subject: String(record.subject || ''),
    original_sent_at: String(record.sentAt || ''),
    original_touch_number: String(record.touchNumber || 1),
    source_history_entry_id: String(record.rootHistoryEntryId || record.historyEntryId || ''),
    selection_index: String(index + 1)
  };
}

function withHistoryFileUrls(entry) {
  const { sourceFile, resultsFile, recipientsFile, ...rest } = entry;

  return {
    ...rest,
    ...(sourceFile
      ? {
          sourceFile: {
            name: sourceFile.name,
            sizeBytes: sourceFile.sizeBytes,
            downloadUrl: `/api/history/${entry.id}/file/source`
          }
        }
      : {}),
    ...(recipientsFile
      ? {
          recipientsFile: {
            recordCount: Number(recipientsFile.recordCount || 0),
            sizeBytes: Number(recipientsFile.sizeBytes || 0)
          }
        }
      : {}),
    ...(resultsFile
      ? {
          resultsFile: {
            name: resultsFile.name,
            sizeBytes: resultsFile.sizeBytes,
            downloadUrl: `/api/history/${entry.id}/file/results`
          }
        }
      : {})
  };
}

async function appendHistoryEntry(entry) {
  const history = await readHistoryEntries();
  history.unshift(entry);

  if (HISTORY_LIMIT > 0 && history.length > HISTORY_LIMIT) {
    history.length = HISTORY_LIMIT;
  }

  await writeHistoryEntries(history);
  return withHistoryFileUrls(entry);
}

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function buildLookup(row, rowNumber) {
  const lookup = new Map();
  lookup.set('row_number', String(rowNumber));

  for (const [rawKey, rawValue] of Object.entries(row || {})) {
    const key = String(rawKey);
    const value = rawValue == null ? '' : String(rawValue);
    const variants = [
      key,
      key.trim(),
      key.toLowerCase(),
      key.trim().toLowerCase(),
      normalizeKey(key)
    ].filter(Boolean);

    for (const variant of variants) {
      if (!lookup.has(variant)) {
        lookup.set(variant, value);
      }
    }
  }

  return lookup;
}

function resolveField(row, fieldName) {
  if (!row || !fieldName) return '';
  const lookup = buildLookup(row, 0);
  const variants = [
    fieldName,
    fieldName.trim(),
    fieldName.toLowerCase(),
    fieldName.trim().toLowerCase(),
    normalizeKey(fieldName)
  ].filter(Boolean);

  for (const variant of variants) {
    if (lookup.has(variant)) {
      return lookup.get(variant);
    }
  }

  return '';
}

function renderTemplate(template, row, rowNumber) {
  if (!template) return '';

  const lookup = buildLookup(row, rowNumber);
  return template.replace(/{{\s*([^{}]+?)\s*}}/g, (_, token) => {
    const rawToken = String(token || '');
    const variants = [
      rawToken,
      rawToken.trim(),
      rawToken.toLowerCase(),
      rawToken.trim().toLowerCase(),
      normalizeKey(rawToken)
    ].filter(Boolean);

    for (const variant of variants) {
      if (lookup.has(variant)) {
        return lookup.get(variant);
      }
    }

    return '';
  });
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function asHtml(text) {
  return escapeHtml(text).replace(/\n/g, '<br>');
}

function buildFromAddress(fromName, fromEmail) {
  const email = String(fromEmail || '').trim();
  const name = String(fromName || '').replace(/["<>]/g, '').trim();

  if (!email) return '';
  if (!name) return email;
  return `${name} <${email}>`;
}

function normalizeEmailList(values = []) {
  const source = Array.isArray(values) ? values : [values];
  return [...new Set(source.map((value) => String(value || '').trim()).filter(Boolean))];
}

function isUserErrorMessage(message) {
  const text = String(message || '').toLowerCase();
  return (
    text.includes('required') ||
    text.includes('unsupported sender type') ||
    text.includes('invalid') ||
    text.includes('must be') ||
    text.includes('app password') ||
    text.includes('not accepted') ||
    text.includes('authentication')
  );
}

async function createSmtpSender(configInput = {}) {
  const host = String(configInput.host || '').trim();
  const port = Number(configInput.port);
  const secure = Boolean(configInput.secure);
  const user = String(configInput.user || '').trim();
  const pass = String(configInput.pass || '');
  const fromName = String(configInput.fromName || '').trim();
  const fromEmail = String(configInput.fromEmail || '').trim() || user;
  const replyTo = String(configInput.replyTo || '').trim();

  if (!host || !port) {
    throw new Error('SMTP host and port are required.');
  }

  if (!fromEmail) {
    throw new Error('SMTP from email or user is required.');
  }

  const transportConfig = {
    host,
    port,
    secure
  };

  if (user || pass) {
    transportConfig.auth = { user, pass };
  }

  const transporter = nodemailer.createTransport(transportConfig);
  await transporter.verify();

  const from = buildFromAddress(fromName, fromEmail);

  return {
    providerType: 'smtp',
    providerName: 'SMTP',
    async send({ to, subject, text, html }) {
      const info = await transporter.sendMail({
        from,
        to,
        subject,
        text,
        html,
        ...(replyTo ? { replyTo } : {})
      });
      return String(info.messageId || '');
    }
  };
}

async function createGmailSender(configInput = {}) {
  const email = String(configInput.email || configInput.user || '').trim();
  const appPassword = String(configInput.appPassword || configInput.pass || '').replace(/\s+/g, '');
  const fromName = String(configInput.fromName || '').trim();
  const fromEmail = String(configInput.fromEmail || '').trim() || email;
  const replyTo = String(configInput.replyTo || '').trim();

  if (!email || !appPassword) {
    throw new Error('Gmail address and app password are required.');
  }

  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: email,
      pass: appPassword
    }
  });

  await transporter.verify();

  const from = buildFromAddress(fromName, fromEmail);

  return {
    providerType: 'gmail',
    providerName: 'Gmail',
    async send({ to, subject, text, html }) {
      const info = await transporter.sendMail({
        from,
        to,
        subject,
        text,
        html,
        ...(replyTo ? { replyTo } : {})
      });

      return String(info.messageId || '');
    }
  };
}

async function createResendSender(configInput = {}) {
  const apiKey = String(configInput.apiKey || '').trim();
  const fromEmailPool = normalizeEmailList(
    Array.isArray(configInput.fromEmails) && configInput.fromEmails.length
      ? configInput.fromEmails
      : [configInput.fromEmail]
  );
  const fromEmail = fromEmailPool[0] || '';
  const fromName = String(configInput.fromName || '').trim();
  const replyTo = String(configInput.replyTo || '').trim();
  let sendCount = 0;

  if (!apiKey || !fromEmail) {
    throw new Error('Resend API key and at least one from email are required.');
  }

  return {
    providerType: 'resend',
    providerName: 'Resend',
    async send({ to, subject, text, html }) {
      const selectedFromEmail =
        fromEmailPool[sendCount % fromEmailPool.length] || fromEmail;
      sendCount += 1;

      const response = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          from: buildFromAddress(fromName, selectedFromEmail),
          to: [to],
          subject,
          text,
          html,
          ...(replyTo ? { reply_to: replyTo } : {})
        })
      });

      const raw = await response.text();
      let data = {};

      try {
        data = raw ? JSON.parse(raw) : {};
      } catch (_error) {
        data = {};
      }

      if (!response.ok) {
        const message =
          data?.message ||
          data?.error?.message ||
          data?.error ||
          raw ||
          'Resend request failed.';
        throw new Error(message);
      }

      return String(data.id || '');
    }
  };
}

async function createSendGridSender(configInput = {}) {
  const apiKey = String(configInput.apiKey || '').trim();
  const fromEmail = String(configInput.fromEmail || '').trim();
  const fromName = String(configInput.fromName || '').trim();
  const replyTo = String(configInput.replyTo || '').trim();

  if (!apiKey || !fromEmail) {
    throw new Error('SendGrid API key and from email are required.');
  }

  return {
    providerType: 'sendgrid',
    providerName: 'SendGrid',
    async send({ to, subject, text, html }) {
      const content = [];

      if (text) {
        content.push({ type: 'text/plain', value: text });
      }

      if (html) {
        content.push({ type: 'text/html', value: html });
      }

      if (!content.length) {
        content.push({ type: 'text/plain', value: '' });
      }

      const response = await fetch('https://api.sendgrid.com/v3/mail/send', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          personalizations: [
            {
              to: [{ email: to }],
              subject
            }
          ],
          from: {
            email: fromEmail,
            ...(fromName ? { name: fromName } : {})
          },
          ...(replyTo ? { reply_to: { email: replyTo } } : {}),
          content
        })
      });

      const raw = await response.text();
      if (!response.ok) {
        let message = raw || 'SendGrid request failed.';

        try {
          const parsed = raw ? JSON.parse(raw) : {};
          if (Array.isArray(parsed.errors) && parsed.errors[0]?.message) {
            message = parsed.errors[0].message;
          }
        } catch (_error) {
          // keep raw as fallback
        }

        throw new Error(message);
      }

      return String(response.headers.get('x-message-id') || '');
    }
  };
}

async function createProviderSender(providerInput = {}) {
  const type = String(providerInput.type || 'smtp').trim().toLowerCase();

  if (type === 'gmail') {
    return createGmailSender(providerInput.gmail || providerInput);
  }

  if (type === 'smtp') {
    return createSmtpSender(providerInput.smtp || providerInput);
  }

  if (type === 'resend') {
    return createResendSender(providerInput.resend || providerInput);
  }

  if (type === 'sendgrid') {
    return createSendGridSender(providerInput.sendgrid || providerInput);
  }

  throw new Error('Unsupported sender type. Use Gmail, SMTP, Resend, or SendGrid.');
}

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

app.get('/api/history', async (_req, res) => {
  try {
    const entries = await readHistoryEntries();
    return res.json({
      entries: entries.map(withHistoryFileUrls)
    });
  } catch (error) {
    return res.status(500).json({
      error: 'Could not load send history.',
      details: error.message
    });
  }
});

app.get('/api/history/:entryId/file/:kind', async (req, res) => {
  try {
    const { entryId, kind } = req.params;
    const entries = await readHistoryEntries();
    const entry = entries.find((item) => item.id === entryId);

    if (!entry) {
      return res.status(404).json({ error: 'History entry not found.' });
    }

    const fileMeta = kind === 'source' ? entry.sourceFile : kind === 'results' ? entry.resultsFile : null;
    if (!fileMeta?.storedFilename) {
      return res.status(404).json({ error: 'Requested file not found.' });
    }

    const filePath = path.join(HISTORY_FILES_DIR, fileMeta.storedFilename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'Stored file is no longer available.' });
    }

    return res.download(filePath, fileMeta.name);
  } catch (error) {
    return res.status(500).json({
      error: 'Could not download history file.',
      details: error.message
    });
  }
});

app.get('/api/sent-emails', async (_req, res) => {
  try {
    const { query = '', status = 'all', minAgeDays = 0, offset = 0, limit = SENT_EMAILS_DEFAULT_LIMIT } =
      _req.query || {};
    const normalizedOffset = parseNonNegativeInteger(offset, 0);
    const normalizedLimit = clampNumber(
      parseNonNegativeInteger(limit, SENT_EMAILS_DEFAULT_LIMIT) || SENT_EMAILS_DEFAULT_LIMIT,
      1,
      SENT_EMAILS_MAX_LIMIT
    );
    const normalizedMinAgeDays = parseNonNegativeInteger(minAgeDays, 0);
    const { records, threads, stats } = await loadSentEmailIndex();
    const filteredThreads = filterSentEmailThreads(threads, {
      query,
      status,
      minAgeDays: normalizedMinAgeDays
    });
    const pageThreads = filteredThreads
      .slice(normalizedOffset, normalizedOffset + normalizedLimit)
      .map(buildSentEmailThreadListItem);

    return res.json({
      stats: {
        ...stats,
        filteredThreads: filteredThreads.length,
        filteredReady: filteredThreads.filter((thread) => isThreadEligibleForFollowUp(thread, normalizedMinAgeDays))
          .length
      },
      page: {
        offset: normalizedOffset,
        limit: normalizedLimit,
        returned: pageThreads.length,
        hasMore: normalizedOffset + pageThreads.length < filteredThreads.length
      },
      filters: {
        query: String(query || ''),
        status: String(status || 'all'),
        minAgeDays: normalizedMinAgeDays
      },
      threads: pageThreads
    });
  } catch (error) {
    return res.status(500).json({
      error: 'Could not load sent emails.',
      details: error.message
    });
  }
});

app.get('/api/sent-emails/:threadId', async (req, res) => {
  try {
    const { threadId } = req.params;
    const { threads } = await loadSentEmailIndex();
    const thread = threads.find((item) => item.id === String(threadId || '').trim());

    if (!thread) {
      return res.status(404).json({ error: 'Sent email thread not found.' });
    }

    return res.json({
      thread: buildSentEmailThreadDetail(thread)
    });
  } catch (error) {
    return res.status(500).json({
      error: 'Could not load sent email thread.',
      details: error.message
    });
  }
});

app.post('/api/follow-up/send', async (req, res) => {
  try {
    const {
      provider,
      recordIds = [],
      subjectTemplate = '',
      bodyTemplate = '',
      delayMs = 0
    } = req.body || {};

    const uniqueRecordIds = [...new Set((Array.isArray(recordIds) ? recordIds : []).map((value) => String(value).trim()).filter(Boolean))];
    if (!uniqueRecordIds.length) {
      return res.status(400).json({ error: 'Select at least one email thread to follow up.' });
    }

    if (!String(subjectTemplate || '').trim() && !String(bodyTemplate || '').trim()) {
      return res.status(400).json({ error: 'Provide a follow-up subject or body.' });
    }

    const { threads } = await loadSentEmailIndex();
    const eligibleThreadMap = new Map(
      threads
        .filter((thread) => thread.latestSuccessfulRecordId && thread.canFollowUp)
        .map((thread) => [thread.latestSuccessfulRecordId, thread])
    );
    const selectedThreads = uniqueRecordIds.map((id) => eligibleThreadMap.get(id)).filter(Boolean);

    if (selectedThreads.length !== uniqueRecordIds.length) {
      return res.status(400).json({
        error: `One or more selected threads are no longer eligible for follow-up. The limit is ${MAX_SUCCESSFUL_TOUCHES} successful touches per thread.`
      });
    }

    const sender = await createProviderSender(provider || { type: 'smtp' });
    const entryId = createId('history_');
    const results = [];
    const recipientRecords = [];
    let sent = 0;
    let failed = 0;

    for (let index = 0; index < selectedThreads.length; index += 1) {
      const baseThread = selectedThreads[index];
      const baseRecord = baseThread.latestSuccessfulRecord;
      const rowNumber = index + 1;
      const context = buildFollowUpContext(baseRecord);
      const renderedSubject = renderTemplate(subjectTemplate, context, baseRecord.rowNumber || rowNumber).trim();
      const subject =
        renderedSubject && renderedSubject.toLowerCase() !== 're:'
          ? renderedSubject
          : `Re: ${String(baseRecord.subject || '(No subject)')}`;
      const textBody = renderTemplate(bodyTemplate, context, baseRecord.rowNumber || rowNumber);
      const htmlBody = asHtml(textBody);
      const sentAt = new Date().toISOString();

      if (!baseRecord.to) {
        failed += 1;
        results.push({
          rowNumber,
          to: '',
          status: 'failed',
          error: 'Missing recipient email on selected thread.',
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: baseRecord.rootHistoryEntryId || baseRecord.historyEntryId || entryId,
            threadId: baseRecord.threadId || baseRecord.id,
            parentRecordId: baseRecord.id || '',
            campaignType: 'followup',
            rowNumber,
            to: '',
            status: 'failed',
            error: 'Missing recipient email on selected thread.',
            subject,
            bodyText: textBody,
            sentAt,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: baseRecord.leadData || {},
            touchNumber: Number(baseRecord.touchNumber || 1) + 1
          })
        );
        continue;
      }

      try {
        const messageId = await sender.send({
          to: baseRecord.to,
          subject,
          text: textBody,
          html: htmlBody
        });

        sent += 1;
        results.push({
          rowNumber,
          to: baseRecord.to,
          status: 'sent',
          messageId,
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: baseRecord.rootHistoryEntryId || baseRecord.historyEntryId || entryId,
            threadId: baseRecord.threadId || baseRecord.id,
            parentRecordId: baseRecord.id || '',
            campaignType: 'followup',
            rowNumber,
            to: baseRecord.to,
            status: 'sent',
            subject,
            bodyText: textBody,
            sentAt,
            messageId,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: baseRecord.leadData || {},
            touchNumber: Number(baseRecord.touchNumber || 1) + 1
          })
        );
      } catch (error) {
        failed += 1;
        results.push({
          rowNumber,
          to: baseRecord.to,
          status: 'failed',
          error: error.message,
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: baseRecord.rootHistoryEntryId || baseRecord.historyEntryId || entryId,
            threadId: baseRecord.threadId || baseRecord.id,
            parentRecordId: baseRecord.id || '',
            campaignType: 'followup',
            rowNumber,
            to: baseRecord.to,
            status: 'failed',
            error: error.message,
            subject,
            bodyText: textBody,
            sentAt,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: baseRecord.leadData || {},
            touchNumber: Number(baseRecord.touchNumber || 1) + 1
          })
        );
      }

      if (delayMs > 0 && index < selectedThreads.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    let historyEntry = null;
    let historyWarning = '';

    try {
      const exportLeads = selectedThreads.map(
        (thread, index) => buildFollowUpExportLead(thread.latestSuccessfulRecord, index)
      );
      const exportColumns = collectColumns(exportLeads);
      const resultsFile = await writeResultsFileToHistory(
        entryId,
        exportColumns,
        exportLeads,
        results,
        'follow-up'
      );
      const recipientsFile = await writeRecipientRecordsFileToHistory(entryId, recipientRecords);

      historyEntry = await appendHistoryEntry({
        id: entryId,
        type: 'followup',
        createdAt: new Date().toISOString(),
        providerType: sender.providerType,
        providerName: sender.providerName,
        delayMs: Number(delayMs || 0),
        subjectTemplate: String(subjectTemplate || ''),
        bodyPreview: summarizeText(bodyTemplate),
        summary: {
          total: selectedThreads.length,
          sent,
          failed
        },
        resultsFile,
        recipientsFile,
        relatedHistoryEntryIds: [
          ...new Set(
            selectedThreads
              .map((thread) => thread.rootHistoryEntryId || thread.latestSuccessfulRecord?.historyEntryId)
              .filter(Boolean)
          )
        ],
        previewResults: results.slice(0, 8).map((item) => ({
          rowNumber: item.rowNumber,
          to: item.to,
          status: item.status,
          subject: item.subject,
          sentAt: item.sentAt,
          error: item.error || ''
        }))
      });
    } catch (storageError) {
      historyWarning = `Follow-ups were sent, but history could not be saved: ${storageError.message}`;
    }

    return res.json({
      summary: {
        total: selectedThreads.length,
        sent,
        failed,
        providerType: sender.providerType,
        providerName: sender.providerName
      },
      results,
      ...(historyEntry ? { historyEntry } : {}),
      ...(historyWarning ? { historyWarning } : {})
    });
  } catch (error) {
    if (isUserErrorMessage(error.message)) {
      return res.status(400).json({
        error: error.message
      });
    }

    return res.status(500).json({
      error: 'Unexpected error while sending follow-ups.',
      details: error.message
    });
  }
});

app.post('/api/parse-csv', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'Please upload a CSV file.' });
    }

    const csvText = req.file.buffer.toString('utf8');
    const headersOnly = parse(csvText, {
      bom: true,
      to_line: 1,
      relax_column_count: true,
      skip_empty_lines: true,
      trim: true
    });

    const columns = Array.isArray(headersOnly[0])
      ? headersOnly[0].map((header) => String(header || '').trim())
      : [];

    const rows = parse(csvText, {
      bom: true,
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true
    });

    let sourceFile = null;
    let warning = '';

    try {
      sourceFile = await saveUploadedCsvFile(req.file);
    } catch (storageError) {
      warning = `CSV loaded, but the source file could not be saved for history: ${storageError.message}`;
    }

    return res.json({
      columns,
      rows,
      rowCount: rows.length,
      sourceFile,
      ...(warning ? { warning } : {})
    });
  } catch (error) {
    return res.status(400).json({
      error: 'Could not parse CSV file.',
      details: error.message
    });
  }
});

app.post('/api/ai-rewrite', async (req, res) => {
  try {
    const {
      openAiApiKey,
      model = 'gpt-4o-mini',
      subject = '',
      draft = '',
      intent = '',
      tone = '',
      constraints = '',
      mode = 'rewrite'
    } = req.body || {};

    if (!openAiApiKey) {
      return res.status(400).json({ error: 'OpenAI API key is required.' });
    }

    if (mode === 'rewrite' && !draft.trim() && !subject.trim()) {
      return res.status(400).json({ error: 'Provide a draft subject or body first.' });
    }

    const systemPrompt = [
      mode === 'generate'
        ? 'You create cold outreach emails from instructions.'
        : 'You rewrite cold outreach email drafts.',
      'Keep placeholders like {{First Name}} exactly as they are.',
      'Do not add fake variables and do not remove existing variables.',
      'If a placeholder exists, preserve exact braces and token spelling.',
      'Make it concise and readable for real outbound email.',
      'Return strict JSON with keys: subject, body.'
    ].join(' ');

    const userPayload = {
      mode,
      intent,
      tone,
      constraints,
      draft: {
        subject,
        body: draft
      }
    };

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${openAiApiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model,
        temperature: 0.6,
        response_format: { type: 'json_object' },
        messages: [
          { role: 'system', content: systemPrompt },
          {
            role: 'user',
            content: `Generate output using this input:\n${JSON.stringify(userPayload, null, 2)}`
          }
        ]
      })
    });

    const data = await response.json();

    if (!response.ok) {
      return res.status(response.status).json({
        error: data.error?.message || 'AI rewrite failed.'
      });
    }

    const content = data.choices?.[0]?.message?.content || '{}';
    let parsed = {};

    try {
      parsed = JSON.parse(content);
    } catch (_error) {
      parsed = {
        subject,
        body: content
      };
    }

    return res.json({
      subject: String(parsed.subject || subject),
      body: String(parsed.body || draft)
    });
  } catch (error) {
    return res.status(500).json({
      error: 'Unexpected error while generating draft.',
      details: error.message
    });
  }
});

app.post('/api/send-test', async (req, res) => {
  try {
    const {
      provider,
      toEmail,
      subjectTemplate,
      bodyTemplate,
      sampleLead = {},
      sampleRowNumber = 1,
      sourceFileId = ''
    } = req.body || {};

    const recipient = String(toEmail || '').trim();
    if (!recipient) {
      return res.status(400).json({ error: 'Test recipient email is required.' });
    }

    if (!subjectTemplate && !bodyTemplate) {
      return res.status(400).json({ error: 'Provide a subject or body before test send.' });
    }

    const sender = await createProviderSender(provider || { type: 'smtp' });
    const entryId = createId('history_');

    const subject = renderTemplate(subjectTemplate, sampleLead, sampleRowNumber) || '(No subject)';
    const textBody = renderTemplate(bodyTemplate, sampleLead, sampleRowNumber);
    const htmlBody = asHtml(textBody);
    const sentAt = new Date().toISOString();

    const messageId = await sender.send({
      to: recipient,
      subject,
      text: textBody,
      html: htmlBody
    });

    let historyEntry = null;
    let historyWarning = '';

    try {
      const sourceFile = await copySourceFileToHistory(entryId, sourceFileId);
      const recipientsFile = await writeRecipientRecordsFileToHistory(entryId, [
        buildRecipientRecord({
          historyEntryId: entryId,
          rootHistoryEntryId: entryId,
          campaignType: 'test',
          rowNumber: Number(sampleRowNumber || 1),
          to: recipient,
          status: 'sent',
          subject,
          bodyText: textBody,
          sentAt,
          messageId,
          providerType: sender.providerType,
          providerName: sender.providerName,
          leadData: sampleLead || {},
          touchNumber: 1
        })
      ]);

      historyEntry = await appendHistoryEntry({
        id: entryId,
        type: 'test',
        createdAt: sentAt,
        providerType: sender.providerType,
        providerName: sender.providerName,
        subject,
        bodyPreview: String(textBody || '').slice(0, 280),
        summary: {
          total: 1,
          sent: 1,
          failed: 0
        },
        recipient,
        sourceFile,
        recipientsFile,
        previewResults: [
          {
            to: recipient,
            status: 'sent',
            subject,
            sentAt,
            messageId
          }
        ]
      });
    } catch (storageError) {
      historyWarning = `Test email was sent, but history could not be saved: ${storageError.message}`;
    }

    return res.json({
      ok: true,
      providerType: sender.providerType,
      providerName: sender.providerName,
      to: recipient,
      subject,
      messageId,
      ...(historyEntry ? { historyEntry } : {}),
      ...(historyWarning ? { historyWarning } : {})
    });
  } catch (error) {
    if (isUserErrorMessage(error.message)) {
      return res.status(400).json({
        error: error.message
      });
    }

    return res.status(500).json({
      error: 'Unexpected error while sending test email.',
      details: error.message
    });
  }
});

app.post('/api/send-bulk', async (req, res) => {
  try {
    const {
      provider,
      smtp = {},
      columns = [],
      leads = [],
      emailColumn,
      subjectTemplate,
      bodyTemplate,
      delayMs = 0,
      sourceFileId = ''
    } = req.body || {};

    const providerInput = provider || { type: 'smtp', smtp };

    if (!emailColumn) {
      return res.status(400).json({ error: 'Select the email column from the CSV.' });
    }

    if (!Array.isArray(leads) || leads.length === 0) {
      return res.status(400).json({ error: 'Upload a leads CSV with at least one row.' });
    }

    if (!subjectTemplate && !bodyTemplate) {
      return res.status(400).json({ error: 'Provide at least a subject or a body template.' });
    }

    const sender = await createProviderSender(providerInput);
    const entryId = createId('history_');

    const results = [];
    const recipientRecords = [];
    let sent = 0;
    let failed = 0;

    for (let i = 0; i < leads.length; i += 1) {
      const row = leads[i];
      const rowNumber = i + 1;
      const to = resolveField(row, emailColumn).trim();
      const subject = renderTemplate(subjectTemplate, row, rowNumber) || '(No subject)';
      const textBody = renderTemplate(bodyTemplate, row, rowNumber);
      const htmlBody = asHtml(textBody);
      const sentAt = new Date().toISOString();

      if (!to) {
        failed += 1;
        results.push({
          rowNumber,
          to: '',
          status: 'failed',
          error: 'Missing recipient email in selected column.',
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: entryId,
            campaignType: 'bulk',
            rowNumber,
            to: '',
            status: 'failed',
            error: 'Missing recipient email in selected column.',
            subject,
            bodyText: textBody,
            sentAt,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: row || {},
            touchNumber: 1
          })
        );
        continue;
      }

      try {
        const messageId = await sender.send({
          to,
          subject,
          text: textBody,
          html: htmlBody
        });

        sent += 1;
        results.push({
          rowNumber,
          to,
          status: 'sent',
          messageId,
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: entryId,
            campaignType: 'bulk',
            rowNumber,
            to,
            status: 'sent',
            subject,
            bodyText: textBody,
            sentAt,
            messageId,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: row || {},
            touchNumber: 1
          })
        );
      } catch (error) {
        failed += 1;
        results.push({
          rowNumber,
          to,
          status: 'failed',
          error: error.message,
          subject,
          provider: sender.providerType,
          sentAt
        });
        recipientRecords.push(
          buildRecipientRecord({
            historyEntryId: entryId,
            rootHistoryEntryId: entryId,
            campaignType: 'bulk',
            rowNumber,
            to,
            status: 'failed',
            error: error.message,
            subject,
            bodyText: textBody,
            sentAt,
            providerType: sender.providerType,
            providerName: sender.providerName,
            leadData: row || {},
            touchNumber: 1
          })
        );
      }

      if (delayMs > 0 && i < leads.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    let historyEntry = null;
    let historyWarning = '';

    try {
      const sourceFile = await copySourceFileToHistory(entryId, sourceFileId);
      const resultsFile = await writeResultsFileToHistory(
        entryId,
        Array.isArray(columns) ? columns : [],
        leads,
        results,
        sourceFile?.name || 'campaign'
      );
      const recipientsFile = await writeRecipientRecordsFileToHistory(entryId, recipientRecords);

      historyEntry = await appendHistoryEntry({
        id: entryId,
        type: 'bulk',
        createdAt: new Date().toISOString(),
        providerType: sender.providerType,
        providerName: sender.providerName,
        emailColumn: String(emailColumn || ''),
        delayMs: Number(delayMs || 0),
        subjectTemplate: String(subjectTemplate || ''),
        bodyPreview: String(bodyTemplate || '').slice(0, 280),
        summary: {
          total: leads.length,
          sent,
          failed
        },
        sourceFile,
        resultsFile,
        recipientsFile,
        previewResults: results.slice(0, 8).map((item) => ({
          rowNumber: item.rowNumber,
          to: item.to,
          status: item.status,
          subject: item.subject,
          sentAt: item.sentAt,
          error: item.error || ''
        }))
      });
    } catch (storageError) {
      historyWarning = `Emails were sent, but history could not be saved: ${storageError.message}`;
    }

    return res.json({
      summary: {
        total: leads.length,
        sent,
        failed,
        providerType: sender.providerType,
        providerName: sender.providerName
      },
      results,
      ...(historyEntry ? { historyEntry } : {}),
      ...(historyWarning ? { historyWarning } : {})
    });
  } catch (error) {
    if (isUserErrorMessage(error.message)) {
      return res.status(400).json({
        error: error.message
      });
    }

    return res.status(500).json({
      error: 'Unexpected error while sending emails.',
      details: error.message
    });
  }
});

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Email Sender running on http://localhost:${PORT}`);
});
