# TODO

Followups parked while we get the first end-to-end run working.

## Post-COG external upload (SFTP)

After a COG worker uploads to S3, also push the file to a remote server via SFTP.
Decisions still open:

- Key type (RSA / ED25519 / auto-detect).
- Host-key pinning vs `AutoAddPolicy`.
- Failure semantics: should an SFTP failure block the STAC upsert, or log-and-continue.

New worker params (proposed):

```
--upload-host
--upload-port              # default 22
--upload-user
--upload-remote-dir
--upload-key-secret-name   # MAAP secret holding PEM private key
```

Implementation sketch lives in chat — `paramiko.SFTPClient.put()` after the S3
upload succeeds. `paramiko` is conda-forge so it slots into the `ingest` env.

## Cleanup old inputs

In the orchestrator's final stage, delete files in the input S3 prefix whose
embedded filename timestamps are older than N days. Opt-in.

Proposed params:

```
--cleanup-input-age-days N
--cleanup-input-regex    # extracts YYYYMMDD or YYYYMMDDThhmmss from filename
--cleanup-dry-run        # log what would be deleted without doing it
```

Safety guardrails to bake in:

- Cross-check against this-run's successful STAC items — never delete an input
  we haven't confirmed got ingested.
- Default to `--cleanup-dry-run` for the first few runs.
- Requires `s3:DeleteObject` on the input bucket for the supplied `role_arn`.

## Scheduled runs

Run the COG pipeline (and eventually the Zarr pipeline) on a daily cron.
MAAP DPS itself doesn't expose a cron UI, so do it externally:

- GitHub Actions workflow on a cron schedule that calls
  `maap.submitJob(algo_id="frozon-iss-cog-pipeline", ...)` via `maap-py`.
- Auth via MAAP token stored as a repo secret.
- Compute the daily `input_s3_prefix` (likely date-partitioned) at submit time.
