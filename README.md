# Email Campaign Sender

Resend-inspired workflow:
1. Audience
2. Content
3. Review & Send

Sender/OpenAI are global in **Sender Settings** (top-right), not part of the pipeline.

Features:
- CSV upload + email column mapping
- Variable templating (`{{First Name}}`, `{{Company}}`, `{{row_number}}`)
- Variable autocomplete on `{{`
- Providers: SMTP, Resend, SendGrid
- AI write/rewrite tools with OpenAI model dropdown
- Test Send before campaign launch
- Live spam-risk warnings in Review
- Bulk send status table + CSV export

Run:

```bash
npm install
npm start
```

Open `http://localhost:3000`
