const express = require('express');
const multer = require('multer');
const nodemailer = require('nodemailer');
const { parse } = require('csv-parse/sync');
const path = require('path');

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }
});

app.use(express.json({ limit: '20mb' }));
app.use(express.static(path.join(__dirname, 'public')));

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

function isUserErrorMessage(message) {
  const text = String(message || '').toLowerCase();
  return (
    text.includes('required') ||
    text.includes('unsupported sender type') ||
    text.includes('invalid') ||
    text.includes('must be')
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

async function createResendSender(configInput = {}) {
  const apiKey = String(configInput.apiKey || '').trim();
  const fromEmail = String(configInput.fromEmail || '').trim();
  const fromName = String(configInput.fromName || '').trim();
  const replyTo = String(configInput.replyTo || '').trim();

  if (!apiKey || !fromEmail) {
    throw new Error('Resend API key and from email are required.');
  }

  const from = buildFromAddress(fromName, fromEmail);

  return {
    providerType: 'resend',
    providerName: 'Resend',
    async send({ to, subject, text, html }) {
      const response = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          from,
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

  if (type === 'smtp') {
    return createSmtpSender(providerInput.smtp || providerInput);
  }

  if (type === 'resend') {
    return createResendSender(providerInput.resend || providerInput);
  }

  if (type === 'sendgrid') {
    return createSendGridSender(providerInput.sendgrid || providerInput);
  }

  throw new Error('Unsupported sender type. Use SMTP, Resend, or SendGrid.');
}

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

app.post('/api/parse-csv', upload.single('file'), (req, res) => {
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

    return res.json({
      columns,
      rows,
      rowCount: rows.length
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
      sampleRowNumber = 1
    } = req.body || {};

    const recipient = String(toEmail || '').trim();
    if (!recipient) {
      return res.status(400).json({ error: 'Test recipient email is required.' });
    }

    if (!subjectTemplate && !bodyTemplate) {
      return res.status(400).json({ error: 'Provide a subject or body before test send.' });
    }

    const sender = await createProviderSender(provider || { type: 'smtp' });

    const subject = renderTemplate(subjectTemplate, sampleLead, sampleRowNumber) || '(No subject)';
    const textBody = renderTemplate(bodyTemplate, sampleLead, sampleRowNumber);
    const htmlBody = asHtml(textBody);

    const messageId = await sender.send({
      to: recipient,
      subject,
      text: textBody,
      html: htmlBody
    });

    return res.json({
      ok: true,
      providerType: sender.providerType,
      providerName: sender.providerName,
      to: recipient,
      subject,
      messageId
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
      leads = [],
      emailColumn,
      subjectTemplate,
      bodyTemplate,
      delayMs = 0
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

    const results = [];
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
      }

      if (delayMs > 0 && i < leads.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    return res.json({
      summary: {
        total: leads.length,
        sent,
        failed,
        providerType: sender.providerType,
        providerName: sender.providerName
      },
      results
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
