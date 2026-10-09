"""Bounded lexical retrieval from reviewed repository documentation, never arbitrary paths."""
from hashlib import sha256
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = ('DATALENS_EVIDENCE.md', 'EVENT_TIME_BUSINESS_LOGIC.md',
             'CORRECTNESS_AND_RECOVERY.md', 'OPERATIONAL_DASHBOARDS_AND_BENCHMARKS.md')
MAX_FILE_BYTES = 100_000
MAX_CHUNKS = 200


class RunbookStore:
    def __init__(self, root=ROOT):
        self.root = Path(root).resolve()

    def search(self, query, limit=4):
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 6:
            raise ValueError('Retrieval limit must be in 1..6')
        terms = set(re.findall(r'[a-z0-9_]+', query.lower()))
        ranked, missing = [], []
        for name in DOCUMENTS:
            target = self.root / name
            # Symlinks cannot expand the reviewed corpus outside the repository.
            if target.is_symlink() or target.resolve().parent != self.root:
                missing.append(name)
                continue
            try:
                with target.open('rb') as handle:
                    raw = handle.read(MAX_FILE_BYTES + 1)
                if len(raw) > MAX_FILE_BYTES:
                    missing.append(name)
                    continue
                lines = raw.decode('utf-8').splitlines()
            except (OSError, UnicodeError):
                missing.append(name)
                continue
            version = sha256(raw).hexdigest()[:16]
            starts = [i for i, line in enumerate(lines) if line.startswith('#')]
            if not starts or starts[0] != 0:
                starts.insert(0, 0)
            starts.append(len(lines))
            chunks = 0
            for start, end in zip(starts, starts[1:]):
                for offset in range(start, end, 16):
                    chunks += 1
                    if chunks > MAX_CHUNKS:
                        break
                    stop = min(offset + 16, end)
                    excerpt = '\n'.join(lines[offset:stop])[:2400]
                    words = set(re.findall(r'[a-z0-9_]+', excerpt.lower()))
                    score = len(terms & words)
                    if score:
                        ref = f'runbook:{name}:{version}:L{offset+1}-L{stop}'
                        ranked.append((score, name, offset, dict(ref=ref, kind='runbook',
                            title=f'{name}, lines {offset+1}–{stop}', content=excerpt,
                            source=name, version=version, line_start=offset+1, line_end=stop)))
                if chunks > MAX_CHUNKS:
                    missing.append(name + ' (chunk limit reached)')
                    break
        ranked.sort(key=lambda item: (-item[0], item[1], item[2]))
        return dict(results=[item[3] for item in ranked[:limit]], missing_documents=missing,
                    semantics='Reviewed documentation, not observations of the running pipeline.')
