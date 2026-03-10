# Railway Deployment

## Service layout
- Create one Railway service from this `backend/` directory.
- Add one Railway PostgreSQL database and attach it to the service.
- Set the service root directory to `backend`.

## Start command
Railway can use `Procfile` automatically.

Equivalent command:

```bash
gunicorn -w 2 -b 0.0.0.0:$PORT wsgi:app
```

## Required environment variables
- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=<long-random-secret>`
- `ADMIN_PASSWORD_HASH=<werkzeug password hash>`
- `ALLOWED_ORIGINS=https://<your-frontend-domain>`
- `SESSION_COOKIE_SECURE=true`
- `SESSION_COOKIE_SAMESITE=None` for cross-site frontend/backend cookies, or `Lax` if same-site
- `ENABLE_SCHEDULED_REFRESH=true`
- `DATABASE_URL=<Railway Postgres URL>`

## Database notes
- `DATABASE_URL` is preferred for Railway. If it is set, the app uses PostgreSQL.
- If `DATABASE_URL` is not set, the app falls back to local `analyses.db` SQLite storage.
- SQLite is fine for local development only.

## Migration from local SQLite
1. Download or copy your local `backend/analyses.db` file.
2. Set `DATABASE_URL` in the Railway service or local shell.
3. Run the migration script once:

```bash
python scripts/migrate_sqlite_to_postgres.py --sqlite-path analyses.db
```

## Health check
Use:

```text
/api/auth/me
```

Expected response when not logged in:

```json
{"isAdmin": false}
```


## Notes
- NOTE: .env.example is reference only and is not auto-loaded on Railway.
- Configure all runtime variables in Railway Variables.

