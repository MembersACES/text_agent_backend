# Wiring text_agent_backend → B4 Climate API

Forwards staged `activity_record.v1` rows to the ACES Climate API (B4), which runs
the PC1 calc and returns real tCO2e. New file: `services/prograde_b4_client.py`
(zero new deps). You add one route to `main.py`.

## 1. Add the route to `main.py`

Near the other `/api/climate/...` routes, paste:

```python
from services.prograde_b4_client import push_report_to_b4, B4PushError

@app.post("/api/climate/entities/{entity_id}/commit-to-b4")
def commit_entity_to_b4(
    entity_id: str,
    period: str = Query("FY26"),
    commit: bool = Query(False, description="lock the report to defensible"),
    jurisdiction: Optional[str] = Query(None, description="state for electricity factor, e.g. VIC"),
    user_info: dict = Depends(verify_roster_access),
    db: Session = Depends(get_db),
):
    """Forward this entity's staged activity to B4 and return the computed report."""
    try:
        return push_report_to_b4(
            db, entity_id, period,
            commit=commit, jurisdiction=jurisdiction,
            user_email=user_info.get("email", "system@acesolutions.com.au"),
        )
    except B4PushError as e:
        raise HTTPException(status_code=502, detail=str(e))
```

(`Query`, `Depends`, `HTTPException`, `Optional`, `Session`, `get_db`,
`verify_roster_access` are already imported in `main.py`.)

## 2. Env vars (dev Cloud Run service for text_agent_backend)

```
B4_BASE_URL=https://<your-aces-climate-api-dev-url>        # no trailing slash
B4_API_KEY=<same value as aces-climate-api API_KEY>        # omit if B4 has no key
```

## 3. Test on dev

```bash
# preview (draft): compute but don't lock
curl -X POST "https://<backend-dev>/api/climate/entities/frankston-rsl/commit-to-b4?period=FY26&jurisdiction=VIC" \
  -H "Authorization: Bearer <google-token>"

# commit (lock -> defensible)
curl -X POST "https://<backend-dev>/api/climate/entities/frankston-rsl/commit-to-b4?period=FY26&commit=true&jurisdiction=VIC" \
  -H "Authorization: Bearer <google-token>"
```

Expected: JSON with `totals_tco2e` (non-zero S1/S2 for Frankston), `run_id`,
`hash_chain`, `status` (`draft` or `locked`), and `skipped_activity` (waste/Scope 3
rows are listed here, never silently zeroed).

## Notes / limits

- **Jurisdiction:** `activity_record.v1` has no state, so B4 defaults electricity to
  VIC (correct for Frankston). Pass `?jurisdiction=NSW` etc. per entity until the ETL
  carries state. Gas/diesel don't need it.
- The push reads `climate_activity_records` filtered by `entity_id` + FY period, so
  the rows must already be staged (your existing "Sync all to SQL" flow).
- Factor values in B4 are dev-test until you swap the gazetted NGA 2025 numbers.
