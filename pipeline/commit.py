#!/usr/bin/env python3
"""Create one historical Git commit per generated Colombian law."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import frontmatter


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_RE = re.compile(r"^\d{4}$")


@dataclass(frozen=True)
class LawFile:
    path: Path
    titulo: str
    identificador: str
    fecha_publicacion: str
    rango: str


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--laws-dir", type=Path, default=Path("laws/co"))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def run_git(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=True,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def ensure_git_repo() -> None:
    try:
        run_git(["git", "rev-parse", "--is-inside-work-tree"])
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = exc.stderr.strip() if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        raise SystemExit(f"error: git repository is not initialized here ({detail})")


def normalize_date(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    if YEAR_RE.fullmatch(text):
        return f"{text}-01-01"
    if DATE_RE.fullmatch(text):
        return text
    return None


def load_law(path: Path) -> LawFile | None:
    try:
        post = frontmatter.load(path)
    except Exception as exc:
        print(f"warning: skipping {path}: frontmatter parse failed: {exc}", file=sys.stderr)
        return None

    fecha_publicacion = normalize_date(post.metadata.get("fecha_publicacion"))
    if not fecha_publicacion:
        print(f"warning: skipping {path}: missing or malformed fecha_publicacion", file=sys.stderr)
        return None

    missing = [
        key
        for key in ("titulo", "identificador", "rango")
        if not str(post.metadata.get(key, "")).strip()
    ]
    if missing:
        print(f"warning: skipping {path}: missing required field(s): {', '.join(missing)}", file=sys.stderr)
        return None

    return LawFile(
        path=path,
        titulo=str(post.metadata["titulo"]).strip(),
        identificador=str(post.metadata["identificador"]).strip(),
        fecha_publicacion=fecha_publicacion,
        rango=str(post.metadata["rango"]).strip(),
    )


def is_already_committed(path: Path) -> bool:
    result = subprocess.run(
        ["git", "log", "--oneline", "--", str(path)],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git log failed for {path}")
    return bool(result.stdout.strip())


def commit_message(law: LawFile) -> str:
    return (
        f"[bootstrap] {law.titulo} — {law.rango}\n\n"
        f"Source-Id: {law.identificador}\n"
        f"Source-Date: {law.fecha_publicacion}\n"
        f"Norm-Id: {law.identificador}"
    )


def commit_law(law: LawFile) -> None:
    env = os.environ.copy()
    git_date = f"{law.fecha_publicacion}T00:00:00"
    env["GIT_AUTHOR_DATE"] = git_date
    env["GIT_COMMITTER_DATE"] = git_date
    env["GIT_COMMITTER_NAME"] = "Legalize"
    env["GIT_COMMITTER_EMAIL"] = "pipeline@legalize.dev"

    subprocess.run(["git", "add", str(law.path)], check=True)
    subprocess.run(["git", "commit", "-m", commit_message(law)], env=env, check=True)


def scan_laws(laws_dir: Path) -> list[LawFile]:
    if not laws_dir.exists():
        raise SystemExit(f"error: laws directory does not exist: {laws_dir}")
    laws = [law for path in sorted(laws_dir.glob("*.md")) if (law := load_law(path))]
    return sorted(laws, key=lambda law: (law.fecha_publicacion, law.identificador))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    ensure_git_repo()

    committed = 0
    skipped = 0

    for law in scan_laws(args.laws_dir):
        try:
            already_committed = is_already_committed(law.path)
        except RuntimeError as exc:
            print(f"warning: skipping {law.path}: {exc}", file=sys.stderr)
            skipped += 1
            continue

        if already_committed:
            print(f"skipping already committed {law.path}")
            skipped += 1
            continue

        if args.dry_run:
            print(f"[DRY RUN] commit {law.path}: {commit_message(law).splitlines()[0]}")
            committed += 1
            continue

        print(f"committing {law.path}: {law.identificador}")
        commit_law(law)
        committed += 1

    print(f"summary: {committed} committed, {skipped} skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
