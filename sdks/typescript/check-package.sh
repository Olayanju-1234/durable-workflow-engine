#!/usr/bin/env bash
# Checks the npm package before it is packed (CI and the release workflow):
# both entry points' types compile, the CommonJS declarations match the ESM
# ones, and require() and import load one and the same module.
set -euo pipefail
cd "$(dirname "$0")"
npx -y -p typescript@5.6.3 tsc --noEmit --strict --module nodenext --moduleResolution nodenext \
  --target es2022 --lib es2022,dom types-check.mts types-check.cts
cmp workflowd.d.mts workflowd.d.cts || { echo "workflowd.d.cts must be a copy of workflowd.d.mts" >&2; exit 1; }
node --input-type=module -e '
import { createRequire } from "node:module";
import * as esm from "workflowd";
const cjs = createRequire(import.meta.url)("workflowd");
for (const name of Object.keys(cjs)) {
  if (esm[name] !== cjs[name]) throw new Error(`import and require disagree on ${name}`);
}
if (Object.keys(esm).length !== Object.keys(cjs).length) throw new Error("import and require export different names");
console.log("workflowd: import and require share", Object.keys(cjs).length, "exports");
'
