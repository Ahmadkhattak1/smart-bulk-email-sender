const STORAGE_KEY = 'email-sender.settings.v3';
const SIDEBAR_COLLAPSED_KEY = 'email-sender.sidebar-collapsed.v1';
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
  currentStep: 'audience',
  activeDraftField: null,
  selectedCsvFile: null
};

const el = {
  appShell: document.querySelector('.app-shell'),
  sidebar: document.getElementById('sidebar'),
  sidebarToggleBtn: document.getElementById('sidebarToggleBtn'),
  sidebarSettingsBtn: document.getElementById('sidebarSettingsBtn'),
  stepTabs: Array.from(document.querySelectorAll('.step-tab')),
  panels: Object.fromEntries(
    STEP_ORDER.map((step) => [step, document.getElementById(`panel-${step}`)])
  ),

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
  resendFromEmail: document.getElementById('resendFromEmail'),
  resendFromName: document.getElementById('resendFromName'),
  resendReplyTo: document.getElementById('resendReplyTo'),
  sendgridApiKey: document.getElementById('sendgridApiKey'),
  sendgridFromEmail: document.getElementById('sendgridFromEmail'),
  sendgridFromName: document.getElementById('sendgridFromName'),
  sendgridReplyTo: document.getElementById('sendgridReplyTo'),
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
  statusBody: document.querySelector('#statusTable tbody')
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

function defaultSettings() {
  return {
    providerType: 'smtp',
    smtpPreset: 'gmail',
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
  return {
    providerType: raw.providerType || base.providerType,
    smtpPreset: raw.smtpPreset || base.smtpPreset,
    smtp: { ...base.smtp, ...(raw.smtp || {}) },
    resend: { ...base.resend, ...(raw.resend || {}) },
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

function loadSidebarCollapsed() {
  try {
    return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
  } catch (_error) {
    return false;
  }
}

function providerLabel(type) {
  return { smtp: 'SMTP', resend: 'Resend', sendgrid: 'SendGrid' }[type] || 'SMTP';
}

function getPrimaryFromEmail(settings) {
  if (settings.providerType === 'smtp') {
    return settings.smtp.fromEmail || settings.smtp.user || '';
  }
  if (settings.providerType === 'resend') {
    return settings.resend.fromEmail || '';
  }
  return settings.sendgrid.fromEmail || '';
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
  return normalizeSettings({
    providerType: el.providerType.value,
    smtpPreset: el.smtpPreset.value,
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
      fromEmail: el.resendFromEmail.value.trim(),
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

  el.smtpHost.value = settings.smtp.host || '';
  el.smtpPort.value = settings.smtp.port || '';
  el.smtpSecure.checked = Boolean(settings.smtp.secure);
  el.smtpUser.value = settings.smtp.user || '';
  el.smtpPass.value = settings.smtp.pass || '';
  el.fromName.value = settings.smtp.fromName || '';
  el.fromEmail.value = settings.smtp.fromEmail || '';
  el.smtpReplyTo.value = settings.smtp.replyTo || '';

  el.resendApiKey.value = settings.resend.apiKey || '';
  el.resendFromEmail.value = settings.resend.fromEmail || '';
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
  const provider = providerLabel(settings.providerType);
  const fromEmail = getPrimaryFromEmail(settings);
  const summarySuffix = fromEmail ? ` · from ${fromEmail}` : '';
  el.topStatus.textContent = `${state.leads.length} leads loaded${summarySuffix}`;

  if (settings.providerType === 'smtp') {
    const ok = settings.smtp.host && settings.smtp.port && (settings.smtp.fromEmail || settings.smtp.user);
    el.providerStatus.textContent = ok ? `${settings.smtp.host}:${settings.smtp.port}` : 'Missing SMTP fields';
  } else if (settings.providerType === 'resend') {
    const ok = settings.resend.apiKey && settings.resend.fromEmail;
    el.providerStatus.textContent = ok ? settings.resend.fromEmail : 'Missing Resend fields';
  } else {
    const ok = settings.sendgrid.apiKey && settings.sendgrid.fromEmail;
    el.providerStatus.textContent = ok ? settings.sendgrid.fromEmail : 'Missing SendGrid fields';
  }

  el.openAiStatus.textContent = settings.openai.apiKey ? 'Configured' : 'Not set';

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
  if (settings.providerType === 'smtp') {
    if (!settings.smtp.host || !settings.smtp.port) {
      throw new Error('SMTP host and port are required in Settings.');
    }
    if (!settings.smtp.fromEmail && !settings.smtp.user) {
      throw new Error('SMTP from email or SMTP user is required in Settings.');
    }
  }

  if (settings.providerType === 'resend') {
    if (!settings.resend.apiKey || !settings.resend.fromEmail) {
      throw new Error('Resend API key and from email are required in Settings.');
    }
  }

  if (settings.providerType === 'sendgrid') {
    if (!settings.sendgrid.apiKey || !settings.sendgrid.fromEmail) {
      throw new Error('SendGrid API key and from email are required in Settings.');
    }
  }
}

function buildProviderPayload(settings) {
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

    setColumnOptions(state.columns);
    renderVariables();
    renderLeadsPreview();
    renderStatusTable([]);

    el.mappingRow.hidden = state.columns.length === 0;
    el.metricLeads.textContent = String(state.leads.length);
    el.metricColumns.textContent = String(state.columns.length);
    el.csvInfo.textContent = `Loaded ${data.rowCount} leads.`;
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
  el.smtpHost,
  el.smtpPort,
  el.smtpSecure,
  el.smtpUser,
  el.smtpPass,
  el.fromName,
  el.fromEmail,
  el.smtpReplyTo,
  el.resendApiKey,
  el.resendFromEmail,
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
        sampleRowNumber: 1
      })
    });

    el.summary.textContent = `Test email sent to ${data.to} via ${data.providerName}.`;
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
        leads: state.leads,
        emailColumn: el.emailColumn.value,
        subjectTemplate: el.subjectTemplate.value,
        bodyTemplate: el.bodyTemplate.value,
        delayMs: Number(el.delayMs.value || 0)
      })
    });

    state.results = data.results || [];
    renderStatusTable(state.results);

    const summary = data.summary || {};
    el.summary.textContent = `Done. Total: ${summary.total || 0}, Sent: ${summary.sent || 0}, Failed: ${summary.failed || 0}.`;
    el.exportBtn.disabled = !state.results.length;
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

(function init() {
  const settings = loadSettings();
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
})();
