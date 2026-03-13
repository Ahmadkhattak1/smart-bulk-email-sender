const STORAGE_KEY = 'email-sender.settings.v3';
const SIDEBAR_COLLAPSED_KEY = 'email-sender.sidebar-collapsed.v1';
const FOLLOW_UP_QUEUE_STORAGE_KEY = 'email-sender.follow-up-queues.v1';
const STEP_ORDER = ['audience', 'content', 'review'];

const SMTP_PRESETS = {
  gmail: { host: 'smtp.gmail.com', port: 587, secure: false },
  outlook: { host: 'smtp.office365.com', port: 587, secure: false },
  yahoo: { host: 'smtp.mail.yahoo.com', port: 465, secure: true },
  zoho: { host: 'smtp.zoho.com', port: 587, secure: false }
};

const FREE_EMAIL_DOMAINS = new Set([
  'gmail.com',
  'yahoo.com',
  'outlook.com',
  'hotmail.com',
  'icloud.com',
  'aol.com',
  'proton.me',
  'protonmail.com'
]);

const SPAM_TRIGGER_PATTERNS = [
  'guarantee',
  'risk[- ]?free',
  'act now',
  'limited time',
  'winner',
  'free money',
  'double your',
  '100%'
];

const state = {
  columns: [],
  leads: [],
  results: [],
  history: [],
  sentEmailThreads: [],
  sentEmailStats: {
    totalRecords: 0,
    totalSent: 0,
    totalFailed: 0,
    totalThreads: 0,
    followUpReady: 0,
    filteredThreads: 0,
    filteredReady: 0
  },
  sentEmailPage: {
    offset: 0,
    limit: 25,
    returned: 0,
    hasMore: false
  },
  sentEmailLoading: false,
  sentThreadDetails: new Map(),
  sentThreadDetailRequests: new Map(),
  sentEmailRequestController: null,
  sentEmailSearchTimer: null,
  selectedFollowUpRecordIds: new Set(),
  followUpQueues: [],
  activeFollowUpQueueId: '',
  currentWorkspace: 'campaign',
  currentStep: 'audience',
  activeDraftField: null,
  selectedCsvFile: null,
  sourceFile: null
};

const el = {
  appShell: document.querySelector('.app-shell'),
  sidebar: document.getElementById('sidebar'),
  sidebarCampaignBtn: document.getElementById('sidebarCampaignBtn'),
  sidebarOutreachBtn: document.getElementById('sidebarOutreachBtn'),
  sidebarToggleBtn: document.getElementById('sidebarToggleBtn'),
  sidebarSettingsBtn: document.getElementById('sidebarSettingsBtn'),
  stepTabs: Array.from(document.querySelectorAll('.step-tab')),
  panels: Object.fromEntries(
    STEP_ORDER.map((step) => [step, document.getElementById(`panel-${step}`)])
  ),
  workspaceCampaign: document.getElementById('workspace-campaign'),
  workspaceOutreach: document.getElementById('workspace-outreach'),

  topTitle: document.getElementById('topTitle'),
  topStatus: document.getElementById('topStatus'),
  providerStatus: document.getElementById('providerStatus'),
  openAiStatus: document.getElementById('openAiStatus'),

  csvFile: document.getElementById('csvFile'),
  csvDropZone: document.getElementById('csvDropZone'),
  csvFileName: document.getElementById('csvFileName'),
  browseCsvBtn: document.getElementById('browseCsvBtn'),
  uploadBtn: document.getElementById('uploadBtn'),
  mappingRow: document.getElementById('mappingRow'),
  emailColumn: document.getElementById('emailColumn'),
  delayMs: document.getElementById('delayMs'),
  csvInfo: document.getElementById('csvInfo'),
  metricLeads: document.getElementById('metricLeads'),
  metricColumns: document.getElementById('metricColumns'),
  leadsPreviewWrap: document.getElementById('leadsPreviewWrap'),
  leadsPreviewTableHead: document.querySelector('#leadsPreviewTable thead'),
  leadsPreviewTableBody: document.querySelector('#leadsPreviewTable tbody'),
  toContentBtn: document.getElementById('toContentBtn'),

  subjectTemplate: document.getElementById('subjectTemplate'),
  bodyTemplate: document.getElementById('bodyTemplate'),
  subjectSuggestions: document.getElementById('subjectSuggestions'),
  bodySuggestions: document.getElementById('bodySuggestions'),
  variablesWrap: document.getElementById('variablesWrap'),
  variablesList: document.getElementById('variablesList'),
  toggleAiBtn: document.getElementById('toggleAiBtn'),
  aiPanel: document.getElementById('aiPanel'),
  rewriteIntent: document.getElementById('rewriteIntent'),
  rewriteTone: document.getElementById('rewriteTone'),
  rewriteConstraints: document.getElementById('rewriteConstraints'),
  writeWithAiBtn: document.getElementById('writeWithAiBtn'),
  rewriteBtn: document.getElementById('rewriteBtn'),
  previewLeadInfo: document.getElementById('previewLeadInfo'),
  previewSubject: document.getElementById('previewSubject'),
  previewBody: document.getElementById('previewBody'),
  backToAudienceBtn: document.getElementById('backToAudienceBtn'),
  toReviewBtn: document.getElementById('toReviewBtn'),
  backToContentBtn: document.getElementById('backToContentBtn'),

  providerType: document.getElementById('providerType'),
  providerCards: Array.from(document.querySelectorAll('.provider-card')),
  gmailEmail: document.getElementById('gmailEmail'),
  gmailAppPassword: document.getElementById('gmailAppPassword'),
  gmailFromName: document.getElementById('gmailFromName'),
  gmailFromEmail: document.getElementById('gmailFromEmail'),
  gmailReplyTo: document.getElementById('gmailReplyTo'),
  smtpPreset: document.getElementById('smtpPreset'),
  smtpHost: document.getElementById('smtpHost'),
  smtpPort: document.getElementById('smtpPort'),
  smtpSecure: document.getElementById('smtpSecure'),
  smtpUser: document.getElementById('smtpUser'),
  smtpPass: document.getElementById('smtpPass'),
  fromName: document.getElementById('fromName'),
  fromEmail: document.getElementById('fromEmail'),
  smtpReplyTo: document.getElementById('smtpReplyTo'),
  resendApiKey: document.getElementById('resendApiKey'),
  resendFromEmailList: document.getElementById('resendFromEmailList'),
  resendFromEmailInputs: Array.from(document.querySelectorAll('[data-resend-from-email]')),
  resendFromEmailRemoveButtons: Array.from(
    document.querySelectorAll('.resend-from-email-remove')
  ),
  addResendFromEmailBtn: document.getElementById('addResendFromEmailBtn'),
  resendFromName: document.getElementById('resendFromName'),
  resendReplyTo: document.getElementById('resendReplyTo'),
  sendgridApiKey: document.getElementById('sendgridApiKey'),
  sendgridFromEmail: document.getElementById('sendgridFromEmail'),
  sendgridFromName: document.getElementById('sendgridFromName'),
  sendgridReplyTo: document.getElementById('sendgridReplyTo'),
  gmailSettings: document.getElementById('gmailSettings'),
  smtpSettings: document.getElementById('smtpSettings'),
  resendSettings: document.getElementById('resendSettings'),
  sendgridSettings: document.getElementById('sendgridSettings'),
  openAiApiKey: document.getElementById('openAiApiKey'),
  openAiModel: document.getElementById('openAiModel'),
  saveSettingsBtn: document.getElementById('saveSettingsBtn'),
  settingsDrawer: document.getElementById('settingsDrawer'),
  settingsOverlay: document.getElementById('settingsOverlay'),
  closeSettingsBtn: document.getElementById('closeSettingsBtn'),
  settingsTabProvider: document.getElementById('settingsTabProvider'),
  settingsTabOpenAi: document.getElementById('settingsTabOpenAi'),
  settingsProviderPanel: document.getElementById('settingsProviderPanel'),
  settingsOpenAiPanel: document.getElementById('settingsOpenAiPanel'),

  reviewLeadCount: document.getElementById('reviewLeadCount'),
  reviewProvider: document.getElementById('reviewProvider'),
  reviewDelay: document.getElementById('reviewDelay'),
  reviewPersonalization: document.getElementById('reviewPersonalization'),
  riskBadge: document.getElementById('riskBadge'),
  riskSummary: document.getElementById('riskSummary'),
  riskList: document.getElementById('riskList'),
  testEmail: document.getElementById('testEmail'),
  testSendBtn: document.getElementById('testSendBtn'),
  sendBtn: document.getElementById('sendBtn'),
  exportBtn: document.getElementById('exportBtn'),
  summary: document.getElementById('summary'),
  statusTableWrap: document.getElementById('statusTableWrap'),
  statusBody: document.querySelector('#statusTable tbody'),
  refreshHistoryBtn: document.getElementById('refreshHistoryBtn'),
  historyEmpty: document.getElementById('historyEmpty'),
  historyList: document.getElementById('historyList'),
  refreshSentEmailsBtn: document.getElementById('refreshSentEmailsBtn'),
  sentThreadsMetric: document.getElementById('sentThreadsMetric'),
  sentReadyMetric: document.getElementById('sentReadyMetric'),
  sentSelectedMetric: document.getElementById('sentSelectedMetric'),
  sentRecordsMetric: document.getElementById('sentRecordsMetric'),
  sentEmailSearch: document.getElementById('sentEmailSearch'),
  followUpAgeDays: document.getElementById('followUpAgeDays'),
  followUpStatusFilter: document.getElementById('followUpStatusFilter'),
  selectVisibleFollowUpsBtn: document.getElementById('selectVisibleFollowUpsBtn'),
  selectReadyFollowUpsBtn: document.getElementById('selectReadyFollowUpsBtn'),
  clearFollowUpSelectionBtn: document.getElementById('clearFollowUpSelectionBtn'),
  createQueueFromSelectionBtn: document.getElementById('createQueueFromSelectionBtn'),
  followUpSelectionSummary: document.getElementById('followUpSelectionSummary'),
  newFollowUpQueueBtn: document.getElementById('newFollowUpQueueBtn'),
  followUpQueueList: document.getElementById('followUpQueueList'),
  followUpQueueEmpty: document.getElementById('followUpQueueEmpty'),
  followUpQueueName: document.getElementById('followUpQueueName'),
  followUpQueueAudienceSummary: document.getElementById('followUpQueueAudienceSummary'),
  followUpQueueSourceNote: document.getElementById('followUpQueueSourceNote'),
  useSelectionForQueueBtn: document.getElementById('useSelectionForQueueBtn'),
  followUpSubjectTemplate: document.getElementById('followUpSubjectTemplate'),
  followUpDelayMs: document.getElementById('followUpDelayMs'),
  followUpBodyTemplate: document.getElementById('followUpBodyTemplate'),
  saveFollowUpQueueBtn: document.getElementById('saveFollowUpQueueBtn'),
  duplicateFollowUpQueueBtn: document.getElementById('duplicateFollowUpQueueBtn'),
  deleteFollowUpQueueBtn: document.getElementById('deleteFollowUpQueueBtn'),
  sendFollowUpBtn: document.getElementById('sendFollowUpBtn'),
  sentEmailsEmpty: document.getElementById('sentEmailsEmpty'),
  sentEmailsInfo: document.getElementById('sentEmailsInfo'),
  sentThreadsList: document.getElementById('sentThreadsList'),
  loadMoreSentEmailsBtn: document.getElementById('loadMoreSentEmailsBtn')
};

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

    [
      key,
      key.trim(),
      key.toLowerCase(),
      key.trim().toLowerCase(),
      normalizeKey(key)
    ]
      .filter(Boolean)
      .forEach((variant) => {
        if (!lookup.has(variant)) lookup.set(variant, value);
      });
  }

  return lookup;
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
      if (lookup.has(variant)) return lookup.get(variant);
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

function normalizeEmailList(values = []) {
  const source = Array.isArray(values) ? values : [values];
  return source.map((value) => String(value || '').trim()).filter(Boolean);
}

function defaultSettings() {
  return {
    providerType: 'gmail',
    smtpPreset: 'gmail',
    gmail: {
      email: '',
      appPassword: '',
      fromName: '',
      fromEmail: '',
      replyTo: ''
    },
    smtp: {
      host: SMTP_PRESETS.gmail.host,
      port: SMTP_PRESETS.gmail.port,
      secure: SMTP_PRESETS.gmail.secure,
      user: '',
      pass: '',
      fromName: '',
      fromEmail: '',
      replyTo: ''
    },
    resend: {
      apiKey: '',
      fromEmail: '',
      fromEmails: [],
      fromName: '',
      replyTo: ''
    },
    sendgrid: {
      apiKey: '',
      fromEmail: '',
      fromName: '',
      replyTo: ''
    },
    openai: {
      apiKey: '',
      model: 'gpt-4o-mini'
    }
  };
}

function normalizeSettings(raw = {}) {
  const base = defaultSettings();
  const resend = { ...base.resend, ...(raw.resend || {}) };
  const resendFromEmails = normalizeEmailList(resend.fromEmails.length ? resend.fromEmails : [resend.fromEmail]);

  return {
    providerType: raw.providerType || base.providerType,
    smtpPreset: raw.smtpPreset || base.smtpPreset,
    gmail: { ...base.gmail, ...(raw.gmail || {}) },
    smtp: { ...base.smtp, ...(raw.smtp || {}) },
    resend: {
      ...resend,
      fromEmail: resendFromEmails[0] || '',
      fromEmails: resendFromEmails
    },
    sendgrid: { ...base.sendgrid, ...(raw.sendgrid || {}) },
    openai: { ...base.openai, ...(raw.openai || {}) }
  };
}

function loadSettings() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultSettings();
    return normalizeSettings(JSON.parse(raw));
  } catch (_error) {
    return defaultSettings();
  }
}

function saveSettings(settings) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
}

function readResendFromEmailsFromForm() {
  return normalizeEmailList(el.resendFromEmailInputs.map((input) => input.value));
}

function visibleResendFromEmailCount() {
  return el.resendFromEmailInputs.filter((input) => !input.closest('.resend-from-email-row')?.hidden).length;
}

function applyResendFromEmailsToForm(values = []) {
  const normalized = normalizeEmailList(values).slice(0, el.resendFromEmailInputs.length);
  const visibleCount = Math.min(
    el.resendFromEmailInputs.length,
    Math.max(1, normalized.length || 1)
  );

  el.resendFromEmailInputs.forEach((input, index) => {
    const row = input.closest('.resend-from-email-row');
    if (row) {
      row.hidden = index >= visibleCount;
    }
    input.value = normalized[index] || '';
  });

  syncResendFromEmailUi();
}

function syncResendFromEmailUi() {
  const visibleCount = visibleResendFromEmailCount();
  const canAddMore = visibleCount < el.resendFromEmailInputs.length;
  const visibleInputs = el.resendFromEmailInputs.slice(0, visibleCount);
  const lastVisibleValue = String(visibleInputs[visibleInputs.length - 1]?.value || '').trim();

  el.resendFromEmailRemoveButtons.forEach((button) => {
    const index = Number(button.dataset.resendRemoveIndex || -1);
    const row = button.closest('.resend-from-email-row');
    button.hidden = !row || row.hidden || index <= 0;
  });

  el.addResendFromEmailBtn.hidden = !canAddMore || !lastVisibleValue;
}

function addResendFromEmailField() {
  const visibleCount = visibleResendFromEmailCount();
  if (visibleCount >= el.resendFromEmailInputs.length) return;

  const nextInput = el.resendFromEmailInputs[visibleCount];
  const nextRow = nextInput?.closest('.resend-from-email-row');
  if (nextRow) {
    nextRow.hidden = false;
  }
  nextInput?.focus();
  syncResendFromEmailUi();
}

function removeResendFromEmailField(index) {
  const values = readResendFromEmailsFromForm();
  if (index < 0 || index >= values.length) return;
  values.splice(index, 1);
  applyResendFromEmailsToForm(values);
}

function createClientId(prefix = 'id_') {
  return `${prefix}${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeFollowUpQueue(raw = {}, fallbackName = 'Follow-up Queue') {
  return {
    id: String(raw.id || createClientId('queue_')),
    name: String(raw.name || fallbackName).trim() || fallbackName,
    recordIds: [...new Set((Array.isArray(raw.recordIds) ? raw.recordIds : []).map((value) => String(value).trim()).filter(Boolean))],
    minAgeDays: Math.max(0, Number(raw.minAgeDays || 0)),
    statusFilter: String(raw.statusFilter || 'all'),
    query: String(raw.query || '').trim(),
    subjectTemplate: String(raw.subjectTemplate || ''),
    bodyTemplate: String(raw.bodyTemplate || ''),
    delayMs: Math.max(0, Number(raw.delayMs || 0)),
    createdAt: String(raw.createdAt || new Date().toISOString()),
    updatedAt: String(raw.updatedAt || raw.createdAt || new Date().toISOString())
  };
}

function loadFollowUpQueues() {
  try {
    const raw = localStorage.getItem(FOLLOW_UP_QUEUE_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed)
      ? parsed.map((queue, index) => normalizeFollowUpQueue(queue, `Follow-up Queue ${index + 1}`))
      : [];
  } catch (_error) {
    return [];
  }
}

function saveFollowUpQueues() {
  localStorage.setItem(FOLLOW_UP_QUEUE_STORAGE_KEY, JSON.stringify(state.followUpQueues));
}

function loadSidebarCollapsed() {
  try {
    return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
  } catch (_error) {
    return false;
  }
}

function providerLabel(type) {
  return {
    gmail: 'Gmail',
    smtp: 'SMTP',
    resend: 'Resend',
    sendgrid: 'SendGrid'
  }[type] || 'Gmail';
}

function getPrimaryFromEmail(settings) {
  if (settings.providerType === 'gmail') {
    return settings.gmail.fromEmail || settings.gmail.email || '';
  }
  if (settings.providerType === 'smtp') {
    return settings.smtp.fromEmail || settings.smtp.user || '';
  }
  if (settings.providerType === 'resend') {
    return settings.resend.fromEmails[0] || settings.resend.fromEmail || '';
  }
  return settings.sendgrid.fromEmail || '';
}

function currentSenderLabel() {
  const settings = readSettingsFromForm();
  const fromEmail = getPrimaryFromEmail(settings);
  return fromEmail ? ` · from ${fromEmail}` : '';
}

function updateTopbar() {
  if (state.currentWorkspace === 'outreach') {
    el.topTitle.textContent = 'Outreach';
    el.topStatus.textContent = state.sentEmailLoading
      ? 'Loading reached-out contacts...'
      : `${state.sentEmailStats.totalThreads || 0} contacts tracked${currentSenderLabel()}`;
    return;
  }

  el.topTitle.textContent = 'Campaign Builder';
  el.topStatus.textContent = `${state.leads.length} leads loaded${currentSenderLabel()}`;
}

function setButtonLoading(button, loading, loadingText) {
  if (!button.dataset.originalText) button.dataset.originalText = button.textContent;
  button.disabled = loading;
  button.textContent = loading ? loadingText : button.dataset.originalText;
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || data.details || 'Request failed.');
  return data;
}

function setStep(step) {
  state.currentStep = step;

  el.stepTabs.forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.step === step);
  });

  STEP_ORDER.forEach((name) => {
    el.panels[name].classList.toggle('active', name === step);
  });

  if (step === 'review') {
    updateReviewSummary();
    updateSpamRisk();
  }
}

function setWorkspace(workspace) {
  if (!['campaign', 'outreach'].includes(workspace)) return;

  state.currentWorkspace = workspace;
  el.sidebarCampaignBtn.classList.toggle('active', workspace === 'campaign');
  el.sidebarOutreachBtn.classList.toggle('active', workspace === 'outreach');
  el.workspaceCampaign.classList.toggle('active', workspace === 'campaign');
  el.workspaceOutreach.classList.toggle('active', workspace === 'outreach');
  updateTopbar();
}

function openSettings() {
  el.settingsDrawer.hidden = false;
}

function closeSettings() {
  el.settingsDrawer.hidden = true;
}

function setSidebarCollapsed(collapsed) {
  el.appShell.classList.toggle('sidebar-collapsed', collapsed);
  el.sidebarToggleBtn.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
  el.sidebarToggleBtn.title = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
  localStorage.setItem(SIDEBAR_COLLAPSED_KEY, collapsed ? '1' : '0');
}

function setSettingsTab(tab) {
  const isProvider = tab !== 'openai';

  el.settingsTabProvider.classList.toggle('active', isProvider);
  el.settingsTabProvider.setAttribute('aria-selected', String(isProvider));

  el.settingsTabOpenAi.classList.toggle('active', !isProvider);
  el.settingsTabOpenAi.setAttribute('aria-selected', String(!isProvider));

  el.settingsProviderPanel.hidden = !isProvider;
  el.settingsOpenAiPanel.hidden = isProvider;
}

function readSettingsFromForm() {
  const resendFromEmails = readResendFromEmailsFromForm();

  return normalizeSettings({
    providerType: el.providerType.value,
    smtpPreset: el.smtpPreset.value,
    gmail: {
      email: el.gmailEmail.value.trim(),
      appPassword: el.gmailAppPassword.value,
      fromName: el.gmailFromName.value.trim(),
      fromEmail: el.gmailFromEmail.value.trim(),
      replyTo: el.gmailReplyTo.value.trim()
    },
    smtp: {
      host: el.smtpHost.value.trim(),
      port: Number(el.smtpPort.value || 0),
      secure: el.smtpSecure.checked,
      user: el.smtpUser.value.trim(),
      pass: el.smtpPass.value,
      fromName: el.fromName.value.trim(),
      fromEmail: el.fromEmail.value.trim(),
      replyTo: el.smtpReplyTo.value.trim()
    },
    resend: {
      apiKey: el.resendApiKey.value.trim(),
      fromEmail: resendFromEmails[0] || '',
      fromEmails: resendFromEmails,
      fromName: el.resendFromName.value.trim(),
      replyTo: el.resendReplyTo.value.trim()
    },
    sendgrid: {
      apiKey: el.sendgridApiKey.value.trim(),
      fromEmail: el.sendgridFromEmail.value.trim(),
      fromName: el.sendgridFromName.value.trim(),
      replyTo: el.sendgridReplyTo.value.trim()
    },
    openai: {
      apiKey: el.openAiApiKey.value.trim(),
      model: el.openAiModel.value
    }
  });
}

function applySettingsToForm(settings) {
  el.providerType.value = settings.providerType;
  el.smtpPreset.value = settings.smtpPreset;

  el.gmailEmail.value = settings.gmail.email || '';
  el.gmailAppPassword.value = settings.gmail.appPassword || '';
  el.gmailFromName.value = settings.gmail.fromName || '';
  el.gmailFromEmail.value = settings.gmail.fromEmail || '';
  el.gmailReplyTo.value = settings.gmail.replyTo || '';

  el.smtpHost.value = settings.smtp.host || '';
  el.smtpPort.value = settings.smtp.port || '';
  el.smtpSecure.checked = Boolean(settings.smtp.secure);
  el.smtpUser.value = settings.smtp.user || '';
  el.smtpPass.value = settings.smtp.pass || '';
  el.fromName.value = settings.smtp.fromName || '';
  el.fromEmail.value = settings.smtp.fromEmail || '';
  el.smtpReplyTo.value = settings.smtp.replyTo || '';

  el.resendApiKey.value = settings.resend.apiKey || '';
  applyResendFromEmailsToForm(settings.resend.fromEmails.length ? settings.resend.fromEmails : [settings.resend.fromEmail]);
  el.resendFromName.value = settings.resend.fromName || '';
  el.resendReplyTo.value = settings.resend.replyTo || '';

  el.sendgridApiKey.value = settings.sendgrid.apiKey || '';
  el.sendgridFromEmail.value = settings.sendgrid.fromEmail || '';
  el.sendgridFromName.value = settings.sendgrid.fromName || '';
  el.sendgridReplyTo.value = settings.sendgrid.replyTo || '';

  el.openAiApiKey.value = settings.openai.apiKey || '';

  const modelExists = Array.from(el.openAiModel.options).some(
    (option) => option.value === settings.openai.model
  );
  el.openAiModel.value = modelExists ? settings.openai.model : 'gpt-4o-mini';

  updateProviderSections();
  updateProviderCards();
  updateStatusUi();
}

function updateProviderSections() {
  const provider = el.providerType.value;
  el.gmailSettings.hidden = provider !== 'gmail';
  el.smtpSettings.hidden = provider !== 'smtp';
  el.resendSettings.hidden = provider !== 'resend';
  el.sendgridSettings.hidden = provider !== 'sendgrid';
}

function updateProviderCards() {
  const provider = el.providerType.value;
  el.providerCards.forEach((card) => {
    card.classList.toggle('active', card.dataset.provider === provider);
  });
}

function updateStatusUi() {
  const settings = readSettingsFromForm();

  if (settings.providerType === 'gmail') {
    const ok = settings.gmail.email && settings.gmail.appPassword;
    const fromEmail = settings.gmail.fromEmail || settings.gmail.email || '';
    el.providerStatus.textContent = ok
      ? fromEmail && fromEmail !== settings.gmail.email
        ? `${fromEmail} via ${settings.gmail.email}`
        : settings.gmail.email
      : 'Missing Gmail fields';
  } else if (settings.providerType === 'smtp') {
    const ok = settings.smtp.host && settings.smtp.port && (settings.smtp.fromEmail || settings.smtp.user);
    el.providerStatus.textContent = ok ? `${settings.smtp.host}:${settings.smtp.port}` : 'Missing SMTP fields';
  } else if (settings.providerType === 'resend') {
    const resendFromEmails = normalizeEmailList(
      settings.resend.fromEmails.length ? settings.resend.fromEmails : [settings.resend.fromEmail]
    );
    const ok = settings.resend.apiKey && resendFromEmails.length;
    el.providerStatus.textContent = ok
      ? resendFromEmails.length > 1
        ? `${resendFromEmails[0]} +${resendFromEmails.length - 1} rotating`
        : resendFromEmails[0]
      : 'Missing Resend fields';
  } else {
    const ok = settings.sendgrid.apiKey && settings.sendgrid.fromEmail;
    el.providerStatus.textContent = ok ? settings.sendgrid.fromEmail : 'Missing SendGrid fields';
  }

  el.openAiStatus.textContent = settings.openai.apiKey ? 'Configured' : 'Not set';
  updateTopbar();

  updateReviewSummary();
  updateSpamRisk();
}

function applySmtpPreset(preset) {
  const config = SMTP_PRESETS[preset];
  if (!config) return;
  el.smtpHost.value = config.host;
  el.smtpPort.value = config.port;
  el.smtpSecure.checked = Boolean(config.secure);
}

function validateProviderSettings(settings) {
  if (settings.providerType === 'gmail') {
    if (!settings.gmail.email || !settings.gmail.appPassword) {
      throw new Error('Gmail address and app password are required in Settings.');
    }
  }

  if (settings.providerType === 'smtp') {
    if (!settings.smtp.host || !settings.smtp.port) {
      throw new Error('SMTP host and port are required in Settings.');
    }
    if (!settings.smtp.fromEmail && !settings.smtp.user) {
      throw new Error('SMTP from email or SMTP user is required in Settings.');
    }
  }

  if (settings.providerType === 'resend') {
    const resendFromEmails = normalizeEmailList(
      settings.resend.fromEmails.length ? settings.resend.fromEmails : [settings.resend.fromEmail]
    );
    if (!settings.resend.apiKey || !resendFromEmails.length) {
      throw new Error('Resend API key and at least one from email are required in Settings.');
    }
  }

  if (settings.providerType === 'sendgrid') {
    if (!settings.sendgrid.apiKey || !settings.sendgrid.fromEmail) {
      throw new Error('SendGrid API key and from email are required in Settings.');
    }
  }
}

function buildProviderPayload(settings) {
  if (settings.providerType === 'gmail') return { type: 'gmail', gmail: settings.gmail };
  if (settings.providerType === 'smtp') return { type: 'smtp', smtp: settings.smtp };
  if (settings.providerType === 'resend') return { type: 'resend', resend: settings.resend };
  return { type: 'sendgrid', sendgrid: settings.sendgrid };
}

function detectEmailColumn(columns) {
  const exact = columns.find((column) => String(column).trim().toLowerCase() === 'email');
  if (exact) return exact;

  return (
    columns.find((column) => String(column).trim().toLowerCase().includes('email')) || columns[0]
  );
}

function setColumnOptions(columns) {
  el.emailColumn.innerHTML = '';

  columns.forEach((column) => {
    const option = document.createElement('option');
    option.value = column;
    option.textContent = column;
    el.emailColumn.appendChild(option);
  });

  const preferred = detectEmailColumn(columns);
  if (preferred) el.emailColumn.value = preferred;
}

function renderLeadsPreview() {
  if (!state.leads.length || !state.columns.length) {
    el.leadsPreviewWrap.hidden = true;
    el.leadsPreviewTableHead.innerHTML = '';
    el.leadsPreviewTableBody.innerHTML = '';
    return;
  }

  const columns = state.columns.slice(0, 6);
  const rows = state.leads.slice(0, 5);

  el.leadsPreviewTableHead.innerHTML = `<tr>${columns
    .map((col) => `<th>${escapeHtml(col)}</th>`)
    .join('')}</tr>`;

  el.leadsPreviewTableBody.innerHTML = rows
    .map((row) => {
      const cells = columns
        .map((col) => `<td>${escapeHtml(row[col] ?? '')}</td>`)
        .join('');
      return `<tr>${cells}</tr>`;
    })
    .join('');

  el.leadsPreviewWrap.hidden = false;
}

function variableNames() {
  const base = [...state.columns];
  if (!base.includes('row_number')) base.push('row_number');
  return base;
}

function insertAtCursor(input, text) {
  const start = input.selectionStart || 0;
  const end = input.selectionEnd || 0;
  const before = input.value.slice(0, start);
  const after = input.value.slice(end);
  input.value = `${before}${text}${after}`;
  const cursor = start + text.length;
  input.setSelectionRange(cursor, cursor);
  input.focus();
}

function renderVariables() {
  el.variablesList.innerHTML = '';
  const names = variableNames();

  names.forEach((name) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = `{{${name}}}`;
    button.addEventListener('click', () => {
      const target = state.activeDraftField || el.bodyTemplate;
      insertAtCursor(target, `{{${name}}}`);
      updatePreview();
      updateSpamRisk();
    });
    el.variablesList.appendChild(button);
  });

  el.variablesWrap.hidden = names.length === 0;
}

function getVariableSuggestionContext(input) {
  const cursor = input.selectionStart || 0;
  const before = input.value.slice(0, cursor);
  const openIndex = before.lastIndexOf('{{');

  if (openIndex === -1) return null;

  const closeIndex = before.lastIndexOf('}}');
  if (closeIndex > openIndex) return null;

  const queryRaw = before.slice(openIndex + 2);
  if (queryRaw.includes('\n')) return null;

  return {
    start: openIndex,
    end: cursor,
    query: queryRaw.trim().toLowerCase()
  };
}

function hideSuggestionMenu(menu) {
  menu.hidden = true;
  menu.innerHTML = '';
}

function applyVariableSuggestion(input, name, context) {
  const token = `{{${name}}}`;
  const before = input.value.slice(0, context.start);
  const after = input.value.slice(context.end);
  input.value = `${before}${token}${after}`;
  const cursor = before.length + token.length;
  input.setSelectionRange(cursor, cursor);
  input.focus();
  updatePreview();
  updateSpamRisk();
}

function showVariableSuggestions(input, menu) {
  const context = getVariableSuggestionContext(input);
  if (!context) {
    hideSuggestionMenu(menu);
    return;
  }

  const matches = variableNames().filter((name) =>
    name.toLowerCase().includes(context.query)
  );

  if (!matches.length) {
    hideSuggestionMenu(menu);
    return;
  }

  menu.innerHTML = '';

  matches.slice(0, 12).forEach((name) => {
    const item = document.createElement('button');
    item.type = 'button';
    item.className = 'suggestion-item';
    item.textContent = `{{${name}}}`;

    item.addEventListener('mousedown', (event) => {
      event.preventDefault();
      applyVariableSuggestion(input, name, context);
      hideSuggestionMenu(menu);
    });

    menu.appendChild(item);
  });

  menu.hidden = false;
}

function bindVariableAutocomplete(input, menu) {
  input.addEventListener('input', () => showVariableSuggestions(input, menu));
  input.addEventListener('click', () => showVariableSuggestions(input, menu));
  input.addEventListener('keydown', () => {
    setTimeout(() => showVariableSuggestions(input, menu), 0);
  });
  input.addEventListener('blur', () => {
    setTimeout(() => hideSuggestionMenu(menu), 120);
  });
}

function updatePreview() {
  const firstLead = state.leads[0] || {};
  const hasLead = state.leads.length > 0;

  const renderedSubject = hasLead
    ? renderTemplate(el.subjectTemplate.value, firstLead, 1)
    : el.subjectTemplate.value;

  const renderedBody = hasLead
    ? renderTemplate(el.bodyTemplate.value, firstLead, 1)
    : el.bodyTemplate.value;

  el.previewLeadInfo.textContent = hasLead
    ? 'Using first lead row for preview.'
    : 'Upload leads to preview resolved variables.';

  el.previewSubject.textContent = renderedSubject || '(No subject)';
  el.previewBody.textContent = renderedBody || '(No body)';
}

function extractDomain(email) {
  const value = String(email || '').trim().toLowerCase();
  if (!value.includes('@')) return '';
  return value.split('@').pop() || '';
}

function analyzeSpamRisk() {
  const settings = readSettingsFromForm();
  const subject = el.subjectTemplate.value.trim();
  const body = el.bodyTemplate.value.trim();
  const combined = `${subject}\n${body}`;
  const leadCount = state.leads.length;
  const delayMs = Number(el.delayMs.value || 0);

  const warnings = [];
  let score = 0;

  const hasPersonalization = /{{\s*[^}]+\s*}}/.test(combined);
  if (!hasPersonalization) {
    score += 2;
    warnings.push('No personalization variables detected.');
  }

  if (leadCount >= 100 && delayMs < 300) {
    score += 3;
    warnings.push('Large list with very low send delay can trigger rate limits and spam flags.');
  } else if (leadCount >= 100 && delayMs < 750) {
    score += 1;
    warnings.push('Consider increasing delay for large sends.');
  }

  if (leadCount >= 500 && delayMs < 1200) {
    score += 2;
    warnings.push('Very high volume needs slower pacing and warm-up.');
  }

  const fromEmail = getPrimaryFromEmail(settings);
  const fromDomain = extractDomain(fromEmail);
  if (fromDomain && FREE_EMAIL_DOMAINS.has(fromDomain)) {
    score += 3;
    warnings.push('Using a free mailbox domain for cold outreach is high risk.');
  }

  const triggerRegex = new RegExp(SPAM_TRIGGER_PATTERNS.join('|'), 'gi');
  const triggerMatches = [...combined.matchAll(triggerRegex)].map((match) =>
    match[0].toLowerCase()
  );

  if (triggerMatches.length) {
    score += 2;
    const unique = [...new Set(triggerMatches)].slice(0, 4).join(', ');
    warnings.push(`Spam-trigger phrases detected: ${unique}.`);
  }

  const linkCount = (combined.match(/https?:\/\/|www\./gi) || []).length;
  if (linkCount > 2) {
    score += 2;
    warnings.push('Too many links in one email can hurt deliverability.');
  }

  const letters = (combined.match(/[A-Za-z]/g) || []).length;
  const caps = (combined.match(/[A-Z]/g) || []).length;
  if (letters > 0 && caps / letters > 0.35) {
    score += 1;
    warnings.push('High uppercase usage can look spammy.');
  }

  if (/!{2,}/.test(combined)) {
    score += 1;
    warnings.push('Multiple exclamation marks can increase spam risk.');
  }

  if (subject.length > 78) {
    score += 1;
    warnings.push('Long subject lines can reduce inbox placement.');
  }

  if (leadCount >= 50 && !/(unsubscribe|opt out|remove me|reply stop)/i.test(body)) {
    score += 2;
    warnings.push('No opt-out wording found for bulk outreach.');
  }

  let level = 'low';
  if (score >= 8) level = 'high';
  else if (score >= 4) level = 'medium';

  if (!warnings.length) {
    warnings.push('No major spam-risk signals detected.');
  }

  return { score, level, warnings };
}

function updateSpamRisk() {
  const risk = analyzeSpamRisk();

  el.riskBadge.classList.remove('risk-low', 'risk-medium', 'risk-high');
  el.riskBadge.classList.add(`risk-${risk.level}`);
  el.riskBadge.textContent = risk.level.toUpperCase();

  if (risk.level === 'low') {
    el.riskSummary.textContent = 'Current content looks relatively safe, keep monitoring domain reputation.';
  } else if (risk.level === 'medium') {
    el.riskSummary.textContent = 'Some settings/content patterns can reduce deliverability. Adjust before launch.';
  } else {
    el.riskSummary.textContent = 'High risk of spam filtering. Fix the warnings before sending at scale.';
  }

  el.riskList.innerHTML = risk.warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join('');

  return risk;
}

function updateReviewSummary() {
  const settings = readSettingsFromForm();
  const hasPersonalization = /{{\s*[^}]+\s*}}/.test(
    `${el.subjectTemplate.value}\n${el.bodyTemplate.value}`
  );

  el.reviewLeadCount.textContent = String(state.leads.length);
  el.reviewProvider.textContent = providerLabel(settings.providerType);
  el.reviewDelay.textContent = `${Number(el.delayMs.value || 0)} ms`;
  el.reviewPersonalization.textContent = hasPersonalization ? 'Yes' : 'No';
}

function renderStatusTable(results) {
  el.statusBody.innerHTML = '';

  results.forEach((row) => {
    const tr = document.createElement('tr');
    const statusClass = row.status === 'sent' ? 'status-sent' : 'status-failed';

    const values = [
      row.rowNumber,
      row.to || '',
      row.status || '',
      row.provider || '',
      row.subject || '',
      row.error || ''
    ];

    values.forEach((value, index) => {
      const td = document.createElement('td');
      td.textContent = String(value || '');
      if (index === 2) td.className = statusClass;
      tr.appendChild(td);
    });

    el.statusBody.appendChild(tr);
  });

  el.statusTableWrap.hidden = results.length === 0;
}

function formatHistoryTimestamp(value) {
  if (!value) return 'Unknown time';

  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(new Date(value));
  } catch (_error) {
    return String(value);
  }
}

function formatFileSize(sizeBytes) {
  const size = Number(sizeBytes || 0);
  if (!size) return '0 B';
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${Math.round(size / 1024)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function historyTypeLabel(type) {
  if (type === 'test') return 'Test Send';
  if (type === 'followup') return 'Follow-up';
  return 'Campaign';
}

function parseDateValue(value) {
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function ageDaysSince(value) {
  const timestamp = parseDateValue(value);
  if (!timestamp) return 0;
  return (Date.now() - timestamp) / (1000 * 60 * 60 * 24);
}

function formatRelativeAge(value) {
  const timestamp = parseDateValue(value);
  if (!timestamp) return 'Unknown age';

  const diffMs = Math.max(0, Date.now() - timestamp);
  const totalMinutes = Math.floor(diffMs / (1000 * 60));
  const totalHours = Math.floor(diffMs / (1000 * 60 * 60));
  const totalDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (totalMinutes < 60) {
    return totalMinutes <= 1 ? 'just now' : `${totalMinutes} min ago`;
  }

  if (totalHours < 24) {
    return totalHours === 1 ? '1 hour ago' : `${totalHours} hours ago`;
  }

  return totalDays === 1 ? '1 day ago' : `${totalDays} days ago`;
}

function getThreadLeadField(thread, candidates = []) {
  const leadData =
    thread?.leadData && !Array.isArray(thread.leadData)
      ? thread.leadData
      : Object.fromEntries(
          (Array.isArray(thread?.leadPreview) ? thread.leadPreview : [])
            .filter((item) => item && item.key)
            .map((item) => [item.key, item.value])
        );
  const keys = Object.keys(leadData);

  for (const candidate of candidates) {
    const found = keys.find((key) => normalizeKey(key) === normalizeKey(candidate));
    if (found && String(leadData[found] || '').trim()) {
      return String(leadData[found]).trim();
    }
  }

  return '';
}

function followUpAgeThreshold() {
  return Number(el.followUpAgeDays?.value || 0);
}

function currentSentEmailFilters() {
  return {
    query: String(el.sentEmailSearch?.value || '').trim(),
    status: String(el.followUpStatusFilter?.value || 'all'),
    minAgeDays: followUpAgeThreshold()
  };
}

function loadedSentEmailThreads() {
  return Array.isArray(state.sentEmailThreads) ? state.sentEmailThreads : [];
}

function isThreadReadyForFollowUp(thread, minAgeDays = followUpAgeThreshold()) {
  if (!thread?.latestSuccessfulRecordId || !thread?.canFollowUp) return false;
  return ageDaysSince(thread.lastAttemptAt || thread.lastSentAt) >= Number(minAgeDays || 0);
}

function followUpRecommendation(thread, minAgeDays = followUpAgeThreshold()) {
  if (!thread?.latestSuccessfulRecordId) {
    return 'No successful send yet';
  }

  if (!thread.canFollowUp || Number(thread.totalSuccessfulSends || 0) >= 3) {
    return 'Touch limit reached';
  }

  const ageDays = ageDaysSince(thread.lastAttemptAt || thread.lastSentAt);
  if (ageDays >= Number(minAgeDays || 0)) {
    return 'Ready now';
  }

  const remaining = Math.max(1, Math.ceil(Number(minAgeDays || 0) - ageDays));
  return remaining === 1 ? 'Wait 1 more day' : `Wait ${remaining} more days`;
}

function filterSentEmailThreads() {
  return loadedSentEmailThreads();
}

function nextFollowUpQueueName() {
  return `Follow-up Queue ${state.followUpQueues.length + 1}`;
}

function followUpStatusLabel(value) {
  return {
    all: 'all contacts',
    ready: 'ready to follow up',
    sent: 'successful threads',
    failed: 'latest attempt failed',
    'first-follow-up': 'no follow-up sent yet'
  }[String(value || 'all')] || 'selected filters';
}

function activeFollowUpQueue() {
  return state.followUpQueues.find((queue) => queue.id === state.activeFollowUpQueueId) || null;
}

function ensureActiveFollowUpQueue() {
  const activeQueue = activeFollowUpQueue();
  if (activeQueue) return activeQueue;

  state.activeFollowUpQueueId = state.followUpQueues[0]?.id || '';
  return activeFollowUpQueue();
}

function queueSourceSummary(queue) {
  const parts = [];
  const ageDays = Number(queue?.minAgeDays || 0);
  const query = String(queue?.query || '').trim();
  const statusFilter = String(queue?.statusFilter || 'all');

  if (ageDays > 0) {
    parts.push(`${ageDays} day${ageDays === 1 ? '' : 's'} or more`);
  }
  if (statusFilter !== 'all') {
    parts.push(followUpStatusLabel(statusFilter));
  }
  if (query) {
    parts.push(`search: ${query}`);
  }

  return parts.length ? parts.join(' | ') : 'Captured without extra filters';
}

function queueAudienceSummary(queue) {
  const total = Array.isArray(queue?.recordIds) ? queue.recordIds.length : 0;
  if (!total) {
    return 'This queue has no contacts yet. Select contacts in Reached Out, then load them into this queue.';
  }

  return `${total} contact${total === 1 ? '' : 's'} in this queue`;
}

function refreshFollowUpStudioState() {
  const queue = ensureActiveFollowUpQueue();
  const hasQueue = Boolean(queue);
  const hasSelection = state.selectedFollowUpRecordIds.size > 0;
  const hasMessage = Boolean(
    String(el.followUpSubjectTemplate?.value || '').trim() || String(el.followUpBodyTemplate?.value || '').trim()
  );

  el.followUpQueueEmpty.hidden = state.followUpQueues.length > 0;
  el.followUpQueueAudienceSummary.textContent = hasQueue
    ? queueAudienceSummary(queue)
    : 'Create or open a queue to choose its audience, message, and delay.';
  el.followUpQueueSourceNote.textContent = hasQueue
    ? `Audience snapshot: ${queueSourceSummary(queue)}.`
    : 'Queue messages are blank by default. Add your own copy and use placeholders where needed.';

  [
    el.followUpQueueName,
    el.followUpSubjectTemplate,
    el.followUpBodyTemplate,
    el.followUpDelayMs
  ].forEach((node) => {
    node.disabled = !hasQueue;
  });

  el.useSelectionForQueueBtn.disabled = !hasQueue || state.sentEmailLoading || !hasSelection;
  el.saveFollowUpQueueBtn.disabled = !hasQueue;
  el.duplicateFollowUpQueueBtn.disabled = !hasQueue;
  el.deleteFollowUpQueueBtn.disabled = !hasQueue;
  el.sendFollowUpBtn.disabled = !hasQueue || state.sentEmailLoading || !queue.recordIds.length || !hasMessage;
}

function applyActiveQueueToEditor() {
  const queue = ensureActiveFollowUpQueue();

  if (!queue) {
    el.followUpQueueName.value = '';
    el.followUpSubjectTemplate.value = '';
    el.followUpBodyTemplate.value = '';
    el.followUpDelayMs.value = '0';
    refreshFollowUpStudioState();
    return;
  }

  el.followUpQueueName.value = queue.name;
  el.followUpSubjectTemplate.value = queue.subjectTemplate;
  el.followUpBodyTemplate.value = queue.bodyTemplate;
  el.followUpDelayMs.value = String(queue.delayMs || 0);
  refreshFollowUpStudioState();
}

function renderFollowUpQueueList() {
  el.followUpQueueList.innerHTML = '';
  el.followUpQueueEmpty.hidden = state.followUpQueues.length > 0;

  state.followUpQueues.forEach((queue) => {
    const item = document.createElement('div');
    item.className = `queue-card ${queue.id === state.activeFollowUpQueueId ? 'active' : ''}`;
    item.dataset.queueId = queue.id;
    item.innerHTML = `
      <button type="button" class="queue-card-main">
        <strong>${escapeHtml(queue.name)}</strong>
        <div class="queue-card-meta">
          <span>${escapeHtml(queueAudienceSummary(queue))}</span>
          <span>${escapeHtml(queueSourceSummary(queue))}</span>
        </div>
      </button>
      <div class="queue-card-actions">
        <button type="button" class="btn btn-ghost queue-delete-btn">Delete</button>
      </div>
    `;

    item.querySelector('.queue-card-main')?.addEventListener('click', () => {
      state.activeFollowUpQueueId = queue.id;
      renderFollowUpQueueList();
      applyActiveQueueToEditor();
    });

    item.querySelector('.queue-delete-btn')?.addEventListener('click', (event) => {
      event.stopPropagation();
      if (state.activeFollowUpQueueId === queue.id) {
        deleteActiveFollowUpQueue();
        return;
      }

      state.followUpQueues = state.followUpQueues.filter((candidate) => candidate.id !== queue.id);
      saveFollowUpQueues();
      renderFollowUpQueueList();
      refreshFollowUpStudioState();
    });

    el.followUpQueueList.appendChild(item);
  });
}

function createFollowUpQueue(queueInput = {}) {
  const queue = normalizeFollowUpQueue(
    {
      ...queueInput,
      id: createClientId('queue_'),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    },
    nextFollowUpQueueName()
  );

  state.followUpQueues = [queue, ...state.followUpQueues];
  state.activeFollowUpQueueId = queue.id;
  saveFollowUpQueues();
  renderFollowUpQueueList();
  applyActiveQueueToEditor();
  el.followUpQueueName.focus();
  el.followUpQueueName.scrollIntoView({ block: 'nearest' });
  return queue;
}

function updateActiveFollowUpQueueFromInputs() {
  const queue = activeFollowUpQueue();
  if (!queue) return;

  queue.name = String(el.followUpQueueName.value || '').trim() || queue.name || nextFollowUpQueueName();
  queue.subjectTemplate = el.followUpSubjectTemplate.value;
  queue.bodyTemplate = el.followUpBodyTemplate.value;
  queue.delayMs = Math.max(0, Number(el.followUpDelayMs.value || 0));
  queue.updatedAt = new Date().toISOString();

  saveFollowUpQueues();
  renderFollowUpQueueList();
  refreshFollowUpStudioState();
}

function createQueueFromThreads(threads = []) {
  const filters = currentSentEmailFilters();
  const recordIds = [...new Set(threads.map((thread) => thread.latestSuccessfulRecordId).filter(Boolean))];
  if (!recordIds.length) {
    alert('Select at least one eligible contact first.');
    return null;
  }

  setWorkspace('outreach');
  return createFollowUpQueue({
    name: nextFollowUpQueueName(),
    recordIds,
    minAgeDays: filters.minAgeDays,
    statusFilter: filters.status,
    query: filters.query,
    subjectTemplate: '',
    bodyTemplate: '',
    delayMs: 0
  });
}

function createQueueFromCurrentSelection() {
  const selectedThreads = loadedSentEmailThreads().filter(
    (thread) =>
      thread.latestSuccessfulRecordId &&
      state.selectedFollowUpRecordIds.has(thread.latestSuccessfulRecordId) &&
      thread.canFollowUp
  );
  createQueueFromThreads(selectedThreads);
}

function createBlankFollowUpQueue() {
  setWorkspace('outreach');
  createFollowUpQueue({
    name: nextFollowUpQueueName(),
    recordIds: [],
    minAgeDays: followUpAgeThreshold(),
    statusFilter: currentSentEmailFilters().status,
    query: currentSentEmailFilters().query
  });
}

function loadSelectionIntoActiveQueue() {
  const queue = activeFollowUpQueue();
  if (!queue) {
    alert('Create a queue first.');
    return;
  }

  const recordIds = [...new Set([...state.selectedFollowUpRecordIds].filter(Boolean))];
  if (!recordIds.length) {
    alert('Select contacts in Reached Out first.');
    return;
  }

  const filters = currentSentEmailFilters();
  queue.recordIds = recordIds;
  queue.minAgeDays = filters.minAgeDays;
  queue.statusFilter = filters.status;
  queue.query = filters.query;
  queue.updatedAt = new Date().toISOString();
  saveFollowUpQueues();
  renderFollowUpQueueList();
  applyActiveQueueToEditor();
}

function duplicateActiveFollowUpQueue() {
  const queue = activeFollowUpQueue();
  if (!queue) {
    alert('Open a queue first.');
    return;
  }

  createFollowUpQueue({
    ...queue,
    name: `${queue.name} Copy`
  });
}

function deleteActiveFollowUpQueue() {
  const queue = activeFollowUpQueue();
  if (!queue) return;
  if (!window.confirm(`Delete "${queue.name}"?`)) return;

  state.followUpQueues = state.followUpQueues.filter((candidate) => candidate.id !== queue.id);
  state.activeFollowUpQueueId = state.followUpQueues[0]?.id || '';
  saveFollowUpQueues();
  renderFollowUpQueueList();
  applyActiveQueueToEditor();
}

function pruneFollowUpSelection() {
  const validIds = new Set(
    loadedSentEmailThreads().map((thread) => thread.latestSuccessfulRecordId).filter(Boolean)
  );
  state.selectedFollowUpRecordIds = new Set(
    [...state.selectedFollowUpRecordIds].filter((id) => validIds.has(id))
  );
}

function updateFollowUpMetrics(visibleThreads = filterSentEmailThreads()) {
  pruneFollowUpSelection();
  const filteredThreadCount = Number.isFinite(Number(state.sentEmailStats.filteredThreads))
    ? Number(state.sentEmailStats.filteredThreads)
    : visibleThreads.length;
  const filteredReadyCount = Number.isFinite(Number(state.sentEmailStats.filteredReady))
    ? Number(state.sentEmailStats.filteredReady)
    : visibleThreads.filter((thread) => isThreadReadyForFollowUp(thread)).length;

  el.sentThreadsMetric.textContent = String(filteredThreadCount);
  el.sentReadyMetric.textContent = String(filteredReadyCount);
  el.sentSelectedMetric.textContent = String(state.selectedFollowUpRecordIds.size);
  el.sentRecordsMetric.textContent = String(state.sentEmailStats.totalRecords || 0);

  const visibleSelectable = visibleThreads.filter(
    (thread) => thread.latestSuccessfulRecordId && thread.canFollowUp
  ).length;
  el.followUpSelectionSummary.textContent = `${state.selectedFollowUpRecordIds.size} selected | ${visibleSelectable} eligible on this page`;
  el.selectVisibleFollowUpsBtn.disabled = state.sentEmailLoading || visibleSelectable === 0;
  el.selectReadyFollowUpsBtn.disabled =
    state.sentEmailLoading || visibleThreads.filter((thread) => isThreadReadyForFollowUp(thread)).length === 0;
  el.clearFollowUpSelectionBtn.disabled = state.sentEmailLoading || state.selectedFollowUpRecordIds.size === 0;
  el.createQueueFromSelectionBtn.disabled = state.sentEmailLoading || state.selectedFollowUpRecordIds.size === 0;
  el.sentEmailsInfo.textContent =
    filteredThreadCount > 0
      ? `Showing ${visibleThreads.length} of ${filteredThreadCount} matching threads.`
      : state.sentEmailLoading
        ? 'Loading matching threads...'
        : state.sentEmailStats.totalThreads > 0
          ? 'No matching threads for the current filters.'
          : 'No sent email threads yet.';
  el.loadMoreSentEmailsBtn.hidden = !state.sentEmailPage.hasMore;
  el.loadMoreSentEmailsBtn.disabled = state.sentEmailLoading || !state.sentEmailPage.hasMore;
  el.sentThreadsList.querySelectorAll('.sent-thread-row').forEach((item) => {
    const thread = loadedSentEmailThreads().find((candidate) => candidate.id === item.dataset.threadId);
    const checkbox = item.querySelector('.sent-thread-checkbox');
    if (!checkbox) return;

    checkbox.disabled = !(thread?.latestSuccessfulRecordId && thread?.canFollowUp && !state.sentEmailLoading);
  });
  refreshFollowUpStudioState();
  updateTopbar();
}

function buildLeadPreviewMarkup(items = []) {
  if (!items.length) return '';

  return `<div class="sent-thread-lead">${items
    .map(
      (item) =>
        `<span class="lead-chip">${escapeHtml(String(item.key || ''))}: ${escapeHtml(
          String(item.value || '')
        )}</span>`
    )
    .join('')}</div>`;
}

function buildSentThreadBodyMarkup(thread, detail = null) {
  const previewItems = Array.isArray(detail?.leadPreview)
    ? detail.leadPreview
    : Array.isArray(thread?.leadPreview)
      ? thread.leadPreview
      : [];
  const detailLeadItems =
    detail && detail.leadData && !Array.isArray(detail.leadData)
      ? Object.entries(detail.leadData)
          .filter(([, value]) => String(value || '').trim())
          .slice(0, 8)
          .map(([key, value]) => ({ key, value }))
      : previewItems;
  const timeline = Array.isArray(detail?.timeline) ? detail.timeline : [];

  return `
    <div class="summary-grid sent-thread-grid">
      <div class="summary-item">
        <span>Last Sent</span>
        <strong>${escapeHtml(formatHistoryTimestamp(thread.lastSentAt || thread.lastAttemptAt))}</strong>
      </div>
      <div class="summary-item">
        <span>Last Attempt</span>
        <strong>${escapeHtml(formatHistoryTimestamp(thread.lastAttemptAt || thread.lastSentAt))}</strong>
      </div>
      <div class="summary-item">
        <span>Successful Sends</span>
        <strong>${escapeHtml(String(thread.totalSuccessfulSends || 0))}</strong>
      </div>
      <div class="summary-item">
        <span>Recommendation</span>
        <strong>${escapeHtml(followUpRecommendation(thread, followUpAgeThreshold()))}</strong>
      </div>
    </div>
    <div class="sent-thread-copy">
      <div><strong>Latest Subject:</strong> ${escapeHtml(thread.lastSubject || '(No subject)')}</div>
      <div><strong>Preview:</strong> ${escapeHtml(thread.lastBodyPreview || '(No body preview)')}</div>
    </div>
    ${buildLeadPreviewMarkup(detailLeadItems)}
    ${
      detail
        ? `<div class="sent-thread-timeline">
            ${
              timeline.length
                ? timeline
                    .map(
                      (event) => `
                        <div class="sent-thread-event">
                          <div class="sent-thread-event-top">
                            <div class="sent-thread-event-meta">
                              <span class="${event.status === 'sent' ? 'status-sent' : 'status-failed'}">${escapeHtml(
                                event.status || ''
                              )}</span>
                              <span>Touch ${escapeHtml(String(event.touchNumber || 1))}</span>
                              <span>${escapeHtml(formatHistoryTimestamp(event.sentAt))}</span>
                            </div>
                            <div class="sent-thread-event-meta">
                              <span>${escapeHtml(
                                event.providerName ||
                                  (event.providerType ? providerLabel(event.providerType) : 'Unknown')
                              )}</span>
                            </div>
                          </div>
                          <div><strong>${escapeHtml(event.subject || '(No subject)')}</strong></div>
                          <div class="sent-thread-event-body">${escapeHtml(
                            event.bodyPreview || '(No body preview)'
                          )}</div>
                          ${
                            event.error
                              ? `<div class="status-failed">${escapeHtml(event.error)}</div>`
                              : ''
                          }
                        </div>
                      `
                    )
                    .join('')
                : '<p class="muted">No timeline events available.</p>'
            }
          </div>`
        : '<p class="muted">Open to load the full thread timeline.</p>'
    }
  `;
}

async function loadSentThreadDetail(threadId) {
  if (state.sentThreadDetails.has(threadId)) {
    return state.sentThreadDetails.get(threadId);
  }

  if (state.sentThreadDetailRequests.has(threadId)) {
    return state.sentThreadDetailRequests.get(threadId);
  }

  const request = requestJson(`/api/sent-emails/${encodeURIComponent(threadId)}`)
    .then((data) => {
      const detail = data.thread || null;
      if (detail) {
        state.sentThreadDetails.set(threadId, detail);
      }
      return detail;
    })
    .finally(() => {
      state.sentThreadDetailRequests.delete(threadId);
    });

  state.sentThreadDetailRequests.set(threadId, request);
  return request;
}

function renderSentEmailThreads() {
  pruneFollowUpSelection();

  const visibleThreads = filterSentEmailThreads();
  updateFollowUpMetrics(visibleThreads);

  el.sentThreadsList.innerHTML = '';
  el.sentEmailsEmpty.hidden = state.sentEmailLoading || visibleThreads.length > 0;

  visibleThreads.forEach((thread) => {
    const item = document.createElement('tr');
    item.className = 'sent-thread-row';
    item.dataset.threadId = thread.id;

    const selected = Boolean(
      thread.latestSuccessfulRecordId &&
        state.selectedFollowUpRecordIds.has(thread.latestSuccessfulRecordId)
    );
    const ready = isThreadReadyForFollowUp(thread);
    const company = thread.company || getThreadLeadField(thread, ['company', 'organization', 'business', 'account']);
    const name = thread.contactName || getThreadLeadField(thread, ['first_name', 'firstname', 'first name', 'name']);
    const selectable = Boolean(thread.latestSuccessfulRecordId && thread.canFollowUp && !state.sentEmailLoading);
    const lastActivity = thread.lastAttemptAt || thread.lastSentAt;

    item.innerHTML = `
      <td class="sent-thread-cell sent-thread-cell-checkbox">
        <label class="sent-thread-checkbox-wrap">
          <input
            class="sent-thread-checkbox"
            type="checkbox"
            ${selected ? 'checked' : ''}
            ${selectable ? '' : 'disabled'}
          />
        </label>
      </td>
      <td class="sent-thread-cell">
        <strong>${escapeHtml(thread.recipient || 'No recipient')}</strong>
        <div class="sent-thread-table-note">${escapeHtml(
          thread.providerName || (thread.providerType ? providerLabel(thread.providerType) : 'Unknown')
        )}</div>
      </td>
      <td class="sent-thread-cell">
        <strong>${escapeHtml(name || 'No contact name')}</strong>
        <div class="sent-thread-table-note">${escapeHtml(company || 'No company')}</div>
      </td>
      <td class="sent-thread-cell">
        <strong>${escapeHtml(thread.lastSubject || '(No subject)')}</strong>
        <div class="sent-thread-table-note">${escapeHtml(thread.lastBodyPreview || '(No preview)')}</div>
      </td>
      <td class="sent-thread-cell">
        <strong>${escapeHtml(formatRelativeAge(lastActivity))}</strong>
        <div class="sent-thread-table-note">${escapeHtml(formatHistoryTimestamp(lastActivity))}</div>
      </td>
      <td class="sent-thread-cell">
        <strong>${escapeHtml(String(thread.totalSuccessfulSends || 0))}</strong>
        <div class="sent-thread-table-note">${escapeHtml(
          thread.status === 'sent' ? 'Latest sent' : 'Latest failed'
        )}</div>
      </td>
      <td class="sent-thread-cell">
        <div class="sent-thread-followup-cell">
          <span class="chip ${ready ? 'chip-ready' : 'chip-muted'}">${escapeHtml(
            followUpRecommendation(thread)
          )}</span>
          <button type="button" class="btn btn-subtle sent-thread-queue-btn" ${
            selectable ? '' : 'disabled'
          }>Queue</button>
        </div>
      </td>
    `;

    const checkbox = item.querySelector('.sent-thread-checkbox');
    checkbox?.addEventListener('change', () => {
      if (!selectable) return;

      if (checkbox.checked) {
        state.selectedFollowUpRecordIds.add(thread.latestSuccessfulRecordId);
      } else {
        state.selectedFollowUpRecordIds.delete(thread.latestSuccessfulRecordId);
      }

      renderSentEmailThreads();
    });

    item.querySelector('.sent-thread-queue-btn')?.addEventListener('click', () => {
      if (!selectable) return;
      createQueueFromThreads([thread]);
    });

    el.sentThreadsList.appendChild(item);
  });
}

async function loadSentEmails(options = {}) {
  const { silent = true, append = false } = options;
  const filters = currentSentEmailFilters();
  const offset = append ? state.sentEmailThreads.length : 0;
  state.sentEmailLoading = true;
  updateFollowUpMetrics(loadedSentEmailThreads());

  if (!append) {
    state.sentEmailThreads = [];
    state.selectedFollowUpRecordIds = new Set();
    state.sentThreadDetails = new Map();
    state.sentThreadDetailRequests = new Map();
    state.sentEmailStats = {
      ...state.sentEmailStats,
      filteredThreads: 0,
      filteredReady: 0
    };
    state.sentEmailPage = {
      ...state.sentEmailPage,
      offset: 0,
      returned: 0,
      hasMore: false
    };
    renderSentEmailThreads();
  }

  if (state.sentEmailRequestController) {
    state.sentEmailRequestController.abort();
  }

  const controller = new AbortController();
  state.sentEmailRequestController = controller;

  try {
    const params = new URLSearchParams({
      query: filters.query,
      status: filters.status,
      minAgeDays: String(filters.minAgeDays),
      offset: String(offset),
      limit: String(state.sentEmailPage.limit || 25)
    });
    const data = await requestJson(`/api/sent-emails?${params.toString()}`, {
      signal: controller.signal
    });
    const nextThreads = Array.isArray(data.threads) ? data.threads : [];

    state.sentEmailThreads = append ? [...state.sentEmailThreads, ...nextThreads] : nextThreads;
    state.sentEmailStats = { ...state.sentEmailStats, ...(data.stats || {}) };
    state.sentEmailPage = {
      offset: Number(data.page?.offset || 0),
      limit: Number(data.page?.limit || state.sentEmailPage.limit || 25),
      returned: Number(data.page?.returned || nextThreads.length),
      hasMore: Boolean(data.page?.hasMore)
    };

    renderSentEmailThreads();
  } catch (error) {
    if (error.name === 'AbortError') {
      return;
    }

    if (!silent) {
      el.summary.textContent = `Could not load sent emails: ${error.message}`;
    }
  } finally {
    state.sentEmailLoading = false;
    if (state.sentEmailRequestController === controller) {
      state.sentEmailRequestController = null;
    }
    updateFollowUpMetrics(loadedSentEmailThreads());
  }
}

function scheduleSentEmailRefresh() {
  if (state.sentEmailSearchTimer) {
    clearTimeout(state.sentEmailSearchTimer);
  }

  state.sentEmailSearchTimer = window.setTimeout(() => {
    loadSentEmails({ silent: false });
  }, 250);
}

async function sendSelectedFollowUps() {
  try {
    const queue = activeFollowUpQueue();
    if (!queue) {
      alert('Create or open a follow-up queue first.');
      return;
    }

    if (!queue.recordIds.length) {
      alert('Add contacts to this queue first.');
      return;
    }

    if (!el.followUpSubjectTemplate.value.trim() && !el.followUpBodyTemplate.value.trim()) {
      alert('Write a follow-up subject or body first.');
      return;
    }

    updateActiveFollowUpQueueFromInputs();

    const settings = readSettingsFromForm();
    validateProviderSettings(settings);

    setButtonLoading(el.sendFollowUpBtn, true, 'Sending queue...');
    refreshFollowUpStudioState();

    const data = await requestJson('/api/follow-up/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: buildProviderPayload(settings),
        recordIds: queue.recordIds,
        subjectTemplate: el.followUpSubjectTemplate.value,
        bodyTemplate: el.followUpBodyTemplate.value,
        delayMs: Number(el.followUpDelayMs.value || 0)
      })
    });

    queue.recordIds = [];
    queue.updatedAt = new Date().toISOString();
    saveFollowUpQueues();
    renderFollowUpQueueList();
    applyActiveQueueToEditor();

    state.results = [];
    el.exportBtn.disabled = true;
    renderStatusTable(data.results || []);

    const summary = data.summary || {};
    el.summary.textContent = data.historyWarning
      ? `Follow-up complete. Total: ${summary.total || 0}, Sent: ${summary.sent || 0}, Failed: ${summary.failed || 0}. ${data.historyWarning}`
      : `Follow-up complete. Total: ${summary.total || 0}, Sent: ${summary.sent || 0}, Failed: ${summary.failed || 0}.`;

    await Promise.all([loadHistory(), loadSentEmails()]);
  } catch (error) {
    el.summary.textContent = `Follow-up send failed: ${error.message}`;
  } finally {
    setButtonLoading(el.sendFollowUpBtn, false, 'Sending queue...');
    refreshFollowUpStudioState();
  }
}

async function loadHistory(options = {}) {
  const { silent = true } = options;

  try {
    const data = await requestJson('/api/history');
    state.history = Array.isArray(data.entries) ? data.entries : [];
    renderHistoryPanel();
  } catch (error) {
    if (!silent) {
      el.summary.textContent = `Could not load history: ${error.message}`;
    }
  }
}

function renderHistoryPanel(entries = state.history) {
  el.historyList.innerHTML = '';
  el.historyEmpty.hidden = entries.length > 0;

  entries.forEach((entry) => {
    const item = document.createElement('details');
    item.className = 'history-item';

    const typeLabel = historyTypeLabel(entry.type);
    const summary = entry.summary || {};
    const previewRows = Array.isArray(entry.previewResults) ? entry.previewResults : [];
    const fileLinks = [];

    if (entry.sourceFile?.downloadUrl) {
      fileLinks.push(
        `<a class="history-link" href="${escapeHtml(entry.sourceFile.downloadUrl)}" download>Source CSV</a><span class="history-file-note">${escapeHtml(entry.sourceFile.name || 'source.csv')} - ${escapeHtml(formatFileSize(entry.sourceFile.sizeBytes))}</span>`
      );
    }

    if (entry.resultsFile?.downloadUrl) {
      fileLinks.push(
        `<a class="history-link" href="${escapeHtml(entry.resultsFile.downloadUrl)}" download>Results CSV</a><span class="history-file-note">${escapeHtml(entry.resultsFile.name || 'results.csv')} - ${escapeHtml(formatFileSize(entry.resultsFile.sizeBytes))}</span>`
      );
    }

    item.innerHTML = `
      <summary class="history-summary">
        <div class="history-title">
          <div class="history-heading-row">
            <strong>${escapeHtml(typeLabel)}</strong>
            <span class="chip">${escapeHtml(entry.providerName || providerLabel(entry.providerType))}</span>
          </div>
          <div class="history-subtitle">${escapeHtml(formatHistoryTimestamp(entry.createdAt))}</div>
        </div>
        <div class="history-stats">
          <span>${escapeHtml(String(summary.sent || 0))} sent</span>
          <span>${escapeHtml(String(summary.failed || 0))} failed</span>
        </div>
      </summary>
      <div class="history-body">
        <div class="summary-grid history-summary-grid">
          <div class="summary-item">
            <span>Type</span>
            <strong>${escapeHtml(typeLabel)}</strong>
          </div>
          <div class="summary-item">
            <span>Total</span>
            <strong>${escapeHtml(String(summary.total || 0))}</strong>
          </div>
          <div class="summary-item">
            <span>Email Column</span>
            <strong>${escapeHtml(entry.emailColumn || 'N/A')}</strong>
          </div>
          <div class="summary-item">
            <span>Delay</span>
            <strong>${escapeHtml(
              entry.type === 'test' ? 'N/A' : `${Number(entry.delayMs || 0)} ms`
            )}</strong>
          </div>
        </div>
        <div class="history-copy">
          <div><strong>Subject:</strong> ${escapeHtml(entry.subject || entry.subjectTemplate || '(No subject)')}</div>
          <div><strong>Body:</strong> ${escapeHtml(entry.bodyPreview || '(No body)')}</div>
        </div>
        ${
          fileLinks.length
            ? `<div class="history-files">${fileLinks
                .map((markup) => `<div class="history-file-row">${markup}</div>`)
                .join('')}</div>`
            : ''
        }
        ${
          previewRows.length
            ? `<div class="history-preview-list">
                ${previewRows
                  .map(
                    (row) => `
                      <div class="history-preview-row">
                        <span class="${row.status === 'sent' ? 'status-sent' : 'status-failed'}">${escapeHtml(
                          row.status || ''
                        )}</span>
                        <span>${escapeHtml(row.to || 'No recipient')}</span>
                        <span>${escapeHtml(formatHistoryTimestamp(row.sentAt))}</span>
                      </div>
                    `
                  )
                  .join('')}
              </div>`
            : ''
        }
      </div>
    `;

    el.historyList.appendChild(item);
  });
}

function toCsvCell(value) {
  const raw = value == null ? '' : String(value);
  const escaped = raw.replace(/"/g, '""');
  const needsQuote = /[",\n]/.test(escaped);
  return needsQuote ? `"${escaped}"` : escaped;
}

function buildResultsCsv() {
  const resultMap = new Map();
  state.results.forEach((result) => resultMap.set(result.rowNumber, result));

  const extraColumns = [
    'send_status',
    'send_error',
    'sent_at',
    'recipient_email',
    'provider_used',
    'subject_used',
    'message_id'
  ];

  const headers = [...state.columns, ...extraColumns];
  const lines = [headers.map(toCsvCell).join(',')];

  state.leads.forEach((lead, index) => {
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

function downloadCsv(filename, csvText) {
  const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

async function runAi(mode) {
  const settings = readSettingsFromForm();
  if (!settings.openai.apiKey) {
    alert('Add OpenAI API key in Settings first.');
    setSettingsTab('openai');
    openSettings();
    return;
  }

  if (mode === 'rewrite' && !el.subjectTemplate.value.trim() && !el.bodyTemplate.value.trim()) {
    alert('Write draft first or use Write With AI.');
    return;
  }

  const button = mode === 'generate' ? el.writeWithAiBtn : el.rewriteBtn;
  const loadingLabel = mode === 'generate' ? 'Writing...' : 'Rewriting...';

  try {
    setButtonLoading(button, true, loadingLabel);

    const data = await requestJson('/api/ai-rewrite', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mode,
        openAiApiKey: settings.openai.apiKey,
        model: settings.openai.model,
        subject: el.subjectTemplate.value,
        draft: el.bodyTemplate.value,
        intent: el.rewriteIntent.value,
        tone: el.rewriteTone.value,
        constraints: el.rewriteConstraints.value
      })
    });

    el.subjectTemplate.value = data.subject || el.subjectTemplate.value;
    el.bodyTemplate.value = data.body || el.bodyTemplate.value;
    el.summary.textContent = mode === 'generate' ? 'AI draft generated.' : 'Draft rewritten.';

    updatePreview();
    updateSpamRisk();
    updateReviewSummary();
  } catch (error) {
    el.summary.textContent = `AI action failed: ${error.message}`;
  } finally {
    setButtonLoading(button, false, loadingLabel);
  }
}

el.stepTabs.forEach((tab) => {
  tab.addEventListener('click', () => {
    setStep(tab.dataset.step);
  });
});

el.sidebarCampaignBtn.addEventListener('click', () => {
  setWorkspace('campaign');
});

el.sidebarOutreachBtn.addEventListener('click', () => {
  setWorkspace('outreach');
});

el.sidebarSettingsBtn.addEventListener('click', () => {
  setSettingsTab('provider');
  openSettings();
});

el.sidebarToggleBtn.addEventListener('click', () => {
  const collapsed = el.appShell.classList.contains('sidebar-collapsed');
  setSidebarCollapsed(!collapsed);
});

el.toContentBtn.addEventListener('click', () => {
  if (!state.leads.length) {
    alert('Load leads before moving to Content.');
    return;
  }
  setStep('content');
});

el.backToAudienceBtn.addEventListener('click', () => setStep('audience'));
el.toReviewBtn.addEventListener('click', () => setStep('review'));
el.backToContentBtn.addEventListener('click', () => setStep('content'));

function setSelectedCsvFile(file) {
  state.selectedCsvFile = file || null;

  if (!file) {
    el.csvFileName.textContent = 'No file selected';
    return;
  }

  const kb = Math.max(1, Math.round(file.size / 1024));
  el.csvFileName.textContent = `${file.name} · ${kb} KB`;
}

async function loadSelectedCsv() {
  try {
    const file = state.selectedCsvFile || el.csvFile.files?.[0];
    if (!file) {
      alert('Choose a CSV file first.');
      return;
    }

    setSelectedCsvFile(file);
    setButtonLoading(el.uploadBtn, true, 'Loading...');

    const formData = new FormData();
    formData.append('file', file);

    const data = await requestJson('/api/parse-csv', {
      method: 'POST',
      body: formData
    });

    state.columns = data.columns || [];
    state.leads = data.rows || [];
    state.results = [];
    state.sourceFile = data.sourceFile || null;

    setColumnOptions(state.columns);
    renderVariables();
    renderLeadsPreview();
    renderStatusTable([]);

    el.mappingRow.hidden = state.columns.length === 0;
    el.metricLeads.textContent = String(state.leads.length);
    el.metricColumns.textContent = String(state.columns.length);
    el.csvInfo.textContent = data.warning
      ? `Loaded ${data.rowCount} leads. ${data.warning}`
      : `Loaded ${data.rowCount} leads.`;
    el.exportBtn.disabled = true;

    updateStatusUi();
    updatePreview();
    updateSpamRisk();
    updateReviewSummary();
  } catch (error) {
    alert(error.message);
  } finally {
    setButtonLoading(el.uploadBtn, false, 'Loading...');
  }
}

el.uploadBtn.addEventListener('click', () => {
  loadSelectedCsv();
});

el.browseCsvBtn.addEventListener('click', (event) => {
  event.stopPropagation();
  el.csvFile.click();
});

el.csvDropZone.addEventListener('click', () => {
  el.csvFile.click();
});

el.csvDropZone.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault();
    el.csvFile.click();
  }
});

el.csvFile.addEventListener('change', () => {
  const file = el.csvFile.files?.[0] || null;
  setSelectedCsvFile(file);
});

['dragenter', 'dragover'].forEach((eventName) => {
  el.csvDropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    el.csvDropZone.classList.add('dragging');
  });
});

['dragleave', 'dragend', 'drop'].forEach((eventName) => {
  el.csvDropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    el.csvDropZone.classList.remove('dragging');
  });
});

el.csvDropZone.addEventListener('drop', (event) => {
  const file = event.dataTransfer?.files?.[0];
  if (!file) return;
  setSelectedCsvFile(file);
});

el.subjectTemplate.addEventListener('focus', () => {
  state.activeDraftField = el.subjectTemplate;
});

el.bodyTemplate.addEventListener('focus', () => {
  state.activeDraftField = el.bodyTemplate;
});

bindVariableAutocomplete(el.subjectTemplate, el.subjectSuggestions);
bindVariableAutocomplete(el.bodyTemplate, el.bodySuggestions);

[el.subjectTemplate, el.bodyTemplate].forEach((node) => {
  node.addEventListener('input', () => {
    updatePreview();
    updateSpamRisk();
    updateReviewSummary();
  });
});

[
  el.sentEmailSearch,
  el.followUpAgeDays,
  el.followUpStatusFilter
].forEach((node) => {
  const eventName = node.tagName === 'SELECT' ? 'change' : 'input';
  node.addEventListener(eventName, () => {
    if (eventName === 'input') {
      scheduleSentEmailRefresh();
      return;
    }

    loadSentEmails({ silent: false });
  });
});

[
  el.followUpQueueName,
  el.followUpSubjectTemplate,
  el.followUpBodyTemplate,
  el.followUpDelayMs
].forEach((node) => {
  const eventName = node.tagName === 'TEXTAREA' || node.type !== 'number' ? 'input' : 'change';
  node.addEventListener(eventName, () => {
    updateActiveFollowUpQueueFromInputs();
  });
});

el.toggleAiBtn.addEventListener('click', () => {
  el.aiPanel.hidden = !el.aiPanel.hidden;
});

el.writeWithAiBtn.addEventListener('click', () => runAi('generate'));
el.rewriteBtn.addEventListener('click', () => runAi('rewrite'));

el.providerCards.forEach((card) => {
  card.addEventListener('click', () => {
    el.providerType.value = card.dataset.provider;
    updateProviderSections();
    updateProviderCards();
    updateStatusUi();
  });
});

el.smtpPreset.addEventListener('change', () => {
  if (el.smtpPreset.value !== 'custom') {
    applySmtpPreset(el.smtpPreset.value);
  }
  updateStatusUi();
});

[
  el.emailColumn,
  el.delayMs,
  el.gmailEmail,
  el.gmailAppPassword,
  el.gmailFromName,
  el.gmailFromEmail,
  el.gmailReplyTo,
  el.smtpHost,
  el.smtpPort,
  el.smtpSecure,
  el.smtpUser,
  el.smtpPass,
  el.fromName,
  el.fromEmail,
  el.smtpReplyTo,
  el.resendApiKey,
  el.resendFromName,
  el.resendReplyTo,
  el.sendgridApiKey,
  el.sendgridFromEmail,
  el.sendgridFromName,
  el.sendgridReplyTo,
  el.openAiApiKey,
  el.openAiModel
].forEach((node) => {
  const eventName =
    node.tagName === 'SELECT' || (node.tagName === 'INPUT' && node.type === 'checkbox')
      ? 'change'
      : 'input';

  node.addEventListener(eventName, () => {
    updateStatusUi();
    updatePreview();
    updateSpamRisk();
  });
});

el.resendFromEmailInputs.forEach((input) => {
  input.addEventListener('input', () => {
    syncResendFromEmailUi();
    updateStatusUi();
    updatePreview();
    updateSpamRisk();
  });
});

el.addResendFromEmailBtn.addEventListener('click', () => {
  addResendFromEmailField();
  updateStatusUi();
});

el.resendFromEmailRemoveButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const index = Number(button.dataset.resendRemoveIndex || -1);
    removeResendFromEmailField(index);
    updateStatusUi();
    updatePreview();
    updateSpamRisk();
  });
});

el.saveSettingsBtn.addEventListener('click', () => {
  const settings = readSettingsFromForm();
  saveSettings(settings);
  updateStatusUi();
  el.summary.textContent = 'Settings saved.';
  closeSettings();
});

el.settingsTabProvider.addEventListener('click', () => setSettingsTab('provider'));
el.settingsTabOpenAi.addEventListener('click', () => setSettingsTab('openai'));

el.closeSettingsBtn.addEventListener('click', () => closeSettings());
el.settingsOverlay.addEventListener('click', () => closeSettings());

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && !el.settingsDrawer.hidden) {
    closeSettings();
  }
});

el.testSendBtn.addEventListener('click', async () => {
  try {
    if (!el.subjectTemplate.value.trim() && !el.bodyTemplate.value.trim()) {
      alert('Write subject or body before test send.');
      setStep('content');
      return;
    }

    const toEmail = el.testEmail.value.trim();
    if (!toEmail) {
      alert('Enter a test recipient email.');
      return;
    }

    const validEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!validEmail.test(toEmail)) {
      alert('Enter a valid test email.');
      return;
    }

    const settings = readSettingsFromForm();
    validateProviderSettings(settings);

    setButtonLoading(el.testSendBtn, true, 'Sending test...');

    const data = await requestJson('/api/send-test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: buildProviderPayload(settings),
        toEmail,
        subjectTemplate: el.subjectTemplate.value,
        bodyTemplate: el.bodyTemplate.value,
        sampleLead: state.leads[0] || {},
        sampleRowNumber: 1,
        sourceFileId: state.sourceFile?.id || ''
      })
    });

    el.summary.textContent = data.historyWarning
      ? `Test email sent to ${data.to} via ${data.providerName}. ${data.historyWarning}`
      : `Test email sent to ${data.to} via ${data.providerName}.`;
    await Promise.all([loadHistory(), loadSentEmails()]);
  } catch (error) {
    el.summary.textContent = `Test send failed: ${error.message}`;
  } finally {
    setButtonLoading(el.testSendBtn, false, 'Sending test...');
  }
});

el.sendBtn.addEventListener('click', async () => {
  try {
    if (!state.leads.length) {
      alert('Upload leads first.');
      setStep('audience');
      return;
    }

    if (!el.emailColumn.value) {
      alert('Select recipient email column in Audience step.');
      setStep('audience');
      return;
    }

    if (!el.subjectTemplate.value.trim() && !el.bodyTemplate.value.trim()) {
      alert('Write subject or body in Content step.');
      setStep('content');
      return;
    }

    const settings = readSettingsFromForm();
    validateProviderSettings(settings);

    const risk = updateSpamRisk();
    if (risk.level === 'high') {
      const confirmHigh = window.confirm(
        'Spam risk is HIGH. Sending now is risky for deliverability. Continue anyway?'
      );
      if (!confirmHigh) return;
    }

    setButtonLoading(el.sendBtn, true, 'Sending...');
    el.summary.textContent = `Sending with ${providerLabel(settings.providerType)}...`;

    const data = await requestJson('/api/send-bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: buildProviderPayload(settings),
        columns: state.columns,
        leads: state.leads,
        emailColumn: el.emailColumn.value,
        subjectTemplate: el.subjectTemplate.value,
        bodyTemplate: el.bodyTemplate.value,
        delayMs: Number(el.delayMs.value || 0),
        sourceFileId: state.sourceFile?.id || ''
      })
    });

    state.results = data.results || [];
    renderStatusTable(state.results);

    const summary = data.summary || {};
    el.summary.textContent = data.historyWarning
      ? `Done. Total: ${summary.total || 0}, Sent: ${summary.sent || 0}, Failed: ${summary.failed || 0}. ${data.historyWarning}`
      : `Done. Total: ${summary.total || 0}, Sent: ${summary.sent || 0}, Failed: ${summary.failed || 0}.`;
    el.exportBtn.disabled = !state.results.length;
    await Promise.all([loadHistory(), loadSentEmails()]);
  } catch (error) {
    el.summary.textContent = `Send failed: ${error.message}`;
  } finally {
    setButtonLoading(el.sendBtn, false, 'Sending...');
  }
});

el.exportBtn.addEventListener('click', () => {
  if (!state.results.length) {
    alert('No campaign results available yet.');
    return;
  }

  const csv = buildResultsCsv();
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  downloadCsv(`campaign-status-${stamp}.csv`, csv);
});

el.refreshHistoryBtn.addEventListener('click', async () => {
  await loadHistory({ silent: false });
});

el.refreshSentEmailsBtn.addEventListener('click', async () => {
  await loadSentEmails({ silent: false });
});

el.selectVisibleFollowUpsBtn.addEventListener('click', () => {
  filterSentEmailThreads().forEach((thread) => {
    if (thread.latestSuccessfulRecordId && thread.canFollowUp) {
      state.selectedFollowUpRecordIds.add(thread.latestSuccessfulRecordId);
    }
  });
  renderSentEmailThreads();
});

el.selectReadyFollowUpsBtn.addEventListener('click', () => {
  filterSentEmailThreads().forEach((thread) => {
    if (thread.latestSuccessfulRecordId && isThreadReadyForFollowUp(thread)) {
      state.selectedFollowUpRecordIds.add(thread.latestSuccessfulRecordId);
    }
  });
  renderSentEmailThreads();
});

el.clearFollowUpSelectionBtn.addEventListener('click', () => {
  state.selectedFollowUpRecordIds = new Set();
  renderSentEmailThreads();
});

el.createQueueFromSelectionBtn.addEventListener('click', () => {
  createQueueFromCurrentSelection();
});

el.newFollowUpQueueBtn.addEventListener('click', () => {
  createBlankFollowUpQueue();
});

el.useSelectionForQueueBtn.addEventListener('click', () => {
  loadSelectionIntoActiveQueue();
});

el.saveFollowUpQueueBtn.addEventListener('click', () => {
  if (!activeFollowUpQueue()) {
    alert('Create a queue first.');
    return;
  }

  updateActiveFollowUpQueueFromInputs();
  el.summary.textContent = 'Follow-up queue saved.';
});

el.duplicateFollowUpQueueBtn.addEventListener('click', () => {
  duplicateActiveFollowUpQueue();
});

el.deleteFollowUpQueueBtn.addEventListener('click', () => {
  deleteActiveFollowUpQueue();
});

el.sendFollowUpBtn.addEventListener('click', async () => {
  await sendSelectedFollowUps();
});

el.loadMoreSentEmailsBtn.addEventListener('click', async () => {
  await loadSentEmails({ silent: false, append: true });
});

(function init() {
  const settings = loadSettings();
  state.followUpQueues = loadFollowUpQueues();
  state.activeFollowUpQueueId = state.followUpQueues[0]?.id || '';
  applySettingsToForm(settings);
  setSidebarCollapsed(loadSidebarCollapsed());

  if (settings.smtpPreset !== 'custom' && (!settings.smtp.host || !settings.smtp.port)) {
    applySmtpPreset(settings.smtpPreset);
  }

  updateProviderSections();
  updateProviderCards();
  setSettingsTab('provider');
  updateStatusUi();
  updatePreview();
  updateSpamRisk();
  renderHistoryPanel();
  renderFollowUpQueueList();
  applyActiveQueueToEditor();
  renderSentEmailThreads();
  setWorkspace('campaign');
  loadHistory();
  loadSentEmails();
})();
