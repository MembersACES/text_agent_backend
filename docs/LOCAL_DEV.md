# Local development — text_agent_backend

## Start server

```powershell
cd "C:\My Projects\text_agent_backend"
uvicorn main:app --reload --port 8000
```

## Required env (.env)

- `BACKEND_API_KEY=test-key` (must match interface + claude-videos publish script)
- `SERVICE_ACCOUNT_FILE` or `SERVICE_ACCOUNT_JSON` for Drive uploads
- `RESOURCES_VIDEOS_FOLDER_ID=1VmTut-4mztUiz95g2BqnZTMoeCVMhsb9`

## Video integration env (auto render after start-from-testimonial)

```
BACKEND_API_KEY=test-key
CZA_VIDEOS_API_URL=http://localhost:8001   # run claude-videos npm run api:dev
CZA_VIDEOS_API_KEY=                        # optional in local dev
# Or without API: auto-spawns sibling ../claude-videos/tools/run_testimonial_pipeline.py
# CLAUDE_VIDEOS_ROOT=C:\My Projects\claude-videos
VIDEO_PUBLISH_ENV=local
```

When `CZA_VIDEOS_API_URL` is set, `POST /api/videos/start-from-testimonial` enqueues
`POST {CZA}/jobs/testimonial` (download → build → render → publish-pack).

## Testimonial video slugs

`POST /api/videos/ingest-testimonial` tries **n8n first**, then **direct Drive upload** (service account) if n8n fails — required for local dev when n8n webhook-test is unavailable.

Ensure one of these is set:
- Member **gdrive_folder_url** (selected on create page), or
- `TESTIMONIAL_STORAGE_FOLDER_ID`, or
- `RESOURCES_VIDEOS_FOLDER_ID` (Interface Videos folder fallback)

Also requires `SERVICE_ACCOUNT_FILE` or `SERVICE_ACCOUNT_JSON` with access to the target folder.

- `GET /api/videos` — list marketing videos (filters: kind, solution_type_id, status)
- `POST /api/videos/upload` — upload MP4 to Interface Videos folder
- `POST /api/videos/publish-pack` — register batch after local render (BACKEND_API_KEY)
- `GET /api/videos/registry` — slug → solution mapping

Pair with interface at `http://localhost:8080` and `NEXT_PUBLIC_API_BASE_URL=http://localhost:8000`.

## Testimonial video slugs

`GET /api/videos/suggest-slug` and the create flow use registry matches when available (e.g. `frankston-gas`). For **new** members, slugs are generated as `{member}-{solution-type}` (e.g. `peninsula-villages-ci-electricity`) — not `draft-{id}`.

`POST /api/videos/start-from-testimonial` creates the Drive pack and auto-starts Step 2 when `CZA_VIDEOS_API_URL` or `CLAUDE_VIDEOS_ROOT` is configured.

## Video ingest troubleshooting
