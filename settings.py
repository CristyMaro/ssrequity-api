import os
#ssr-api/settings.py
SSR_DB_DSN = (os.getenv("SSR_DB_DSN") or "").strip()

# Admin token para endpoints /admin/*
SSR_EQUITY_ADMIN_TOKEN = (os.getenv("SSR_EQUITY_ADMIN_TOKEN") or "").strip()

# Upload limits (bytes)
MAX_UPLOAD_BYTES = int(os.getenv("SSR_MAX_UPLOAD_BYTES", "26214400"))  # 25MB default
