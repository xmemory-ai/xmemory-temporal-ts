import { execFileSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

/**
 * Load the PACKED package the way a consumer does, and check the entry points work.
 *
 * The other two gates never execute the root entry: `check:declarations` only
 * type-checks, and `check:bundle` bundles the workflow subpath. A tarball whose
 * `dist/index.js` throws on import passed both of them — measured, by poisoning one.
 *
 * Both module systems, because the package is CommonJS and half the ecosystem is
 * not: `require()` and `import()` resolve through different `exports` conditions,
 * and a broken one is invisible from the other side.
 */
const CJS = `
const assert = require('node:assert/strict');
const pkg = require('@xmemory/temporal');
assert.equal(typeof pkg.XmemoryPlugin, 'function', 'XmemoryPlugin is not exported');
assert.equal(typeof pkg.TYPE_WRITE_TIMEOUT, 'string', 'the failure types are not exported');
// Constructing it touches the config path without needing a backend or a key.
assert.equal(new pkg.XmemoryPlugin({ instanceId: 'inst-1' }).name, 'xmemory');
console.log('  require("@xmemory/temporal"): ok');
`;

const ESM = `
import assert from 'node:assert/strict';
const pkg = await import('@xmemory/temporal');
assert.equal(typeof pkg.XmemoryPlugin, 'function', 'XmemoryPlugin is not exported');
assert.equal(typeof pkg.TYPE_WRITE_TIMEOUT, 'string', 'the failure types are not exported');
assert.equal(new pkg.XmemoryPlugin({ instanceId: 'inst-1' }).name, 'xmemory');
console.log('  import("@xmemory/temporal"): ok');
`;

async function main(): Promise<void> {
  const root = process.cwd();
  const dir = await mkdtemp(join(tmpdir(), 'xmemory-smoke-'));
  try {
    const given = process.env.XMEMORY_TARBALL;
    const tarball = given
      ? resolve(given)
      : join(
          dir,
          execFileSync('npm', ['pack', '--silent', '--pack-destination', dir], {
            encoding: 'utf8',
            cwd: root,
          }).trim(),
        );
    if (given) console.log(`using the prebuilt tarball ${tarball}`);

    await writeFile(
      join(dir, 'package.json'),
      JSON.stringify({ name: 'smoke-probe', private: true, dependencies: { '@xmemory/temporal': `file:${tarball}` } }),
    );
    execFileSync('npm', ['install', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund', '--silent'], {
      cwd: dir,
      stdio: 'inherit',
    });
    await writeFile(join(dir, 'probe.cjs'), CJS);
    await writeFile(join(dir, 'probe.mjs'), ESM);

    for (const probe of ['probe.cjs', 'probe.mjs']) {
      execFileSync(process.execPath, [join(dir, probe)], { cwd: dir, stdio: 'inherit' });
    }
    console.log('package smoke: ok (both module systems, packed artifact)');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

main().catch((err: unknown) => {
  console.error('package smoke FAILED:', err instanceof Error ? err.message : err);
  process.exit(1);
});
