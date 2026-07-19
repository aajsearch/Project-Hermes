import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def repo_root() -> Path:
    # coinbase_grid_bot/ -> repo root
    return Path(__file__).resolve().parent.parent


def load_repo_env() -> None:
    """
    Load environment variables from the repo-root .env.
    Also loads any .env already present in the current working directory.
    """
    load_dotenv()  # allow exported env vars / local overrides
    load_dotenv(repo_root() / ".env")


def resolve_repo_path(path_value: Optional[str]) -> Optional[str]:
    """
    Resolve a possibly-relative path against the repo root.
    Returns absolute string path, or None if input is empty.
    """
    if not path_value:
        return None
    raw = str(path_value).strip()
    if not raw:
        return None
    p = Path(raw)
    if p.is_absolute():
        return str(p)
    return str((repo_root() / p).resolve())


def get_env_path(var_name: str) -> Optional[str]:
    return resolve_repo_path(os.environ.get(var_name) or None)

