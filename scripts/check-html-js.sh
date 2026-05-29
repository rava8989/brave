#!/bin/bash
# Lint inline <script> blocks in HTML files for reference errors.
# ─────────────────────────────────────────────────────────────────────────
# Catches the class of bug that bit on 2026-05-29:
#   const stradEffSpot = ...   // inside if-block
#   ...                        // reference outside block → ReferenceError
#                              // → page renders "Failed to load"
#
# ESLint's `no-undef` rule catches this: the out-of-block reference has no
# declaration in scope, so ESLint flags it BEFORE the commit lands.
#
# How it works:
#   1. For each staged .html file, extract every <script> block to a temp .js
#   2. Run `npx eslint` against the project's eslint.config.mjs
#   3. Block the commit if ESLint reports any errors
#
# Bypass: `git commit --no-verify` (only do this if you've manually verified
# the lint warning is a false positive — and add the global to
# eslint.config.mjs `globals` if it's a legitimate project-wide name).

set -e
cd "$(dirname "$0")/.."

# Only check staged HTML files (in pre-commit) — or all .html if invoked manually.
if [ -t 0 ] && [ "$1" = "--all" ]; then
  STAGED=$(ls *.html 2>/dev/null)
elif git diff --cached --name-only 2>/dev/null | grep -q '\.html$'; then
  STAGED=$(git diff --cached --name-only --diff-filter=ACM | grep '\.html$' || true)
else
  STAGED=""
fi

if [ -z "$STAGED" ]; then
  exit 0   # nothing to lint
fi

# Sanity check: ESLint must be installed.
if ! command -v npx >/dev/null 2>&1; then
  echo "⚠️  [html-js-lint] npx not found — skipping lint." >&2
  echo "    Install Node.js to enable this check." >&2
  exit 2   # warn, don't block
fi

# Check ESLint is installed locally (via npm install).
if [ ! -f node_modules/.bin/eslint ] && [ ! -f node_modules/eslint/bin/eslint.js ]; then
  echo "⚠️  [html-js-lint] ESLint not installed — run \`npm install\` to enable this check." >&2
  exit 2   # warn, don't block (so a fresh clone before `npm install` doesn't fail)
fi

# ESLint 9 only lints files inside the project root, so extract to a
# subfolder of the repo (gitignored via .gitignore) instead of /tmp.
TMPDIR="$PWD/.eslint-tmp"
rm -rf "$TMPDIR" && mkdir -p "$TMPDIR"
trap "rm -rf $TMPDIR" EXIT

FAILED=0

for html in $STAGED; do
  [ -f "$html" ] || continue

  # Extract every <script> block into ONE concatenated .js file. We do this
  # so that helper functions declared in one <script> tag are visible to
  # references in later tags — ESLint linting blocks individually would
  # false-positive on every cross-block reference (loadChartJS, etc).
  #
  # FIRST strip HTML comments — the literal text "<script>" inside an HTML
  # comment used to fool the regex into treating it as a real script tag.
  basename=$(basename "$html" .html | tr -c 'A-Za-z0-9_' _)
  out_path="$TMPDIR/${basename}.js"
  python3 - "$html" "$out_path" <<'PYEOF'
import re, sys

src, out_path = sys.argv[1], sys.argv[2]
content = open(src).read()

# Strip HTML comments BEFORE matching <script> tags (otherwise "<script>"
# mentioned inside a comment gets parsed as a real opening tag).
content_no_comments = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

# Match <script ...> (NOT with a src=) ... </script>
pattern = re.compile(
    r'<script(?P<attrs>(?:(?!\bsrc=)[^>])*)>(?P<body>.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)

blocks = []
for m in pattern.finditer(content_no_comments):
    body = m.group('body').strip()
    if not body: continue
    attrs = m.group('attrs').lower()
    if 'type=' in attrs and 'javascript' not in attrs and 'module' not in attrs:
        continue   # skip <script type="application/json"> etc.
    blocks.append(body)

if blocks:
    with open(out_path, 'w') as f:
        f.write(f'// concatenated inline JS from {src}\n')
        for i, b in enumerate(blocks, 1):
            f.write(f'\n// ── block {i} ──\n')
            f.write(b)
            f.write('\n')
PYEOF

  [ -f "$out_path" ] || continue

  if ! npx --no-install eslint --config eslint.config.mjs "$out_path" 2>&1; then
    FAILED=1
    echo "" >&2
    echo "🚫 [html-js-lint] ESLint errors in $html — see above." >&2
    echo "   The most common cause: \`const\` or \`let\` declared inside an" >&2
    echo "   if-block, referenced outside the block. Hoist the declaration" >&2
    echo "   above the block." >&2
  fi
done

if [ $FAILED -eq 1 ]; then
  echo "" >&2
  echo "   Bypass (only if false positive — and update eslint.config.mjs):" >&2
  echo "     git commit --no-verify" >&2
  exit 1
fi

echo "✓ [html-js-lint] no reference errors in staged HTML"
exit 0
