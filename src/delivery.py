"""Post-S3 delivery helpers.

Currently exposes one delivery target: SCP push to a remote server.
The step is opt-in (driven by --scp-host); auth uses an SSH private key
resolved from a MAAP secret.

Failure semantics: SCP errors are logged but do NOT block the rest of
the worker. Rationale: the COG is already in S3 and the STAC catalog
should still be emitted so the catalog stays consistent with what's in
S3. Retries belong in a separate workflow.
"""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def upload_via_scp(
    local_path: Path,
    host: str,
    user: str,
    remote_dir: str,
    key_secret_name: str,
    port: int = 22,
    maap_instance=None,
) -> bool:
    """SCP `local_path` to `user@host:remote_dir/<basename>`.

    Returns True on success, False on any failure (the caller should log
    a warning rather than abort). All exceptions are caught and logged.

    Auth: SSH private key resolved from a MAAP secret (PEM format).
    Tries RSA → Ed25519 → ECDSA → DSS in that order.
    """
    try:
        import paramiko
        from scp import SCPClient
    except ImportError as e:
        logger.error(f"SCP dependencies missing ({e}); cannot deliver")
        return False

    from common_utils import MaapUtils
    maap = maap_instance or MaapUtils.get_maap_instance()

    try:
        pem = maap.secrets.get_secret(key_secret_name)
    except Exception as e:
        logger.error(f"Could not fetch SSH key secret '{key_secret_name}': {e}")
        return False
    if not pem:
        logger.error(f"SSH key secret '{key_secret_name}' is empty")
        return False

    pkey = None
    for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key,
                    paramiko.ECDSAKey, paramiko.DSSKey):
        try:
            pkey = key_cls.from_private_key(StringIO(pem))
            logger.info(f"Loaded {key_cls.__name__} private key")
            break
        except paramiko.SSHException:
            continue
    if pkey is None:
        logger.error("Could not parse the SSH private key as RSA/Ed25519/ECDSA/DSS")
        return False

    ssh = paramiko.SSHClient()
    # NOTE: AutoAddPolicy accepts any host key — fine for first-pass dev but
    # we should pin known_hosts before relying on this in production.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        logger.info(f"SSH connecting to {user}@{host}:{port}")
        ssh.connect(host, port=port, username=user, pkey=pkey,
                    timeout=30, allow_agent=False, look_for_keys=False)

        # Ensure the remote directory exists. mkdir -p semantics via `ssh`
        # since SCP itself doesn't create directories.
        _, _, stderr = ssh.exec_command(f"mkdir -p {_shell_quote(remote_dir)}")
        err = stderr.read().decode().strip()
        if err:
            logger.warning(f"Remote mkdir -p stderr: {err}")

        remote_path = f"{remote_dir.rstrip('/')}/{local_path.name}"
        logger.info(f"SCP putting {local_path} → {remote_path}")
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(str(local_path), remote_path)

        ssh.close()
        logger.info("SCP upload OK")
        return True
    except Exception as e:
        logger.error(f"SCP upload failed: {e}", exc_info=True)
        try:
            ssh.close()
        except Exception:
            pass
        return False


def _shell_quote(s: str) -> str:
    """Single-quote-escape for safe use inside a remote shell command."""
    return "'" + s.replace("'", "'\"'\"'") + "'"
