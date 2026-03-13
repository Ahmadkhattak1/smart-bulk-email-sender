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
- Providers: Gmail (Google App Password), SMTP, Resend, SendGrid
- Resend rotating sender pool for bulk sends
- AI write/rewrite tools with OpenAI model dropdown
- Test Send before campaign launch
- Live spam-risk warnings in Review
- Bulk send status table + CSV export
- Persistent send history with source CSV + results CSV downloads

Gmail setup:
1. Open your Google Account security settings.
2. Turn on 2-Step Verification for the Gmail account you will authenticate with.
3. Create an App Password in Google Account > Security > App Passwords.
4. In **Sender Settings**, choose **Gmail**.
5. Enter the Gmail login address, the 16-character app password, and optionally a separate **From Email** for your custom domain.

Notes:
- Use the Gmail app password, not your normal Gmail sign-in password.
- If you want Gmail to send as `you@yourdomain.com`, add that address in Gmail under **Accounts and Import > Send mail as** first.
- Cloudflare Email Routing handles inbound forwarding only. Outbound mail in this app still authenticates through Gmail.

Run:

```bash
npm install
npm start
```

Open `http://localhost:3000`
