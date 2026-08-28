import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

/**
 * Type-check a consumer against the PACKED declarations, in every module mode.
 * Zero errors required.
 *
 * Two decisions worth knowing:
 *
 * * Both module systems: the consumer is compiled as a `.ts` (CommonJS, since the
 *   probe package has no `"type": "module"`) *and* as a `.mts` (always ESM), so the
 *   `import` and `require` halves of the package's `exports` are both exercised.
 * * A **real install** of the real tarball, not a symlink farm. Symlinks resolve by
 *   realpath, so a linked dependency kept resolving types out of *this* repository's
 *   node_modules, and a devDependency silently covered a gap a real consumer hit.
 * * `skipLibCheck: false`, because an error in a published `.d.ts` is reported
 *   inside a declaration file and skipping lib checks would hide the defect this
 *   exists to catch — `node16` consumers, the mode `@temporalio/create` generates,
 *   are where those surface.
 *
 * The fixture installs exactly what a consumer gets: this package and `@types/node`,
 * nothing else. Adding a type package here to quieten an error would make the gate
 * green for a consumer who still fails — `@types/ms` did precisely that.
 *
 * So every diagnostic fails the gate, and one known upstream gap is allowlisted by
 * both path and code (see `KNOWN_UPSTREAM`). The allowlist is deliberately that
 * narrow: an earlier version failed only diagnostics reported inside our own
 * declarations, which let an error in the consumer fixture itself — a missing export,
 * a changed signature, the exact thing this fixture exists to catch — pass as
 * "upstream". A tsc run that fails without emitting a parseable diagnostic fails too.
 *
 * `@types/node` 25 and newer are not checked: Temporal's own declarations do not
 * compile against them (`EventEmitter<[never]>` fails its own constraint, still true
 * in @temporalio/worker 1.22.0), so they can only be used with `skipLibCheck`, which
 * makes this gate prove nothing.
 */
const MODES = ['node16', 'nodenext', 'bundler'] as const;

// Every `@types/node` major this package supports. 25+ is excluded above.
const NODE_TYPE_MAJORS = [22, 24] as const;

/**
 * The one upstream gap a consumer meets with or without this package.
 *
 * `@temporalio/common` references `ms`, which ships no types, so `skipLibCheck:
 * false` reports TS7016 inside its declarations. Matched on path *and* code, so it
 * cannot excuse anything else — including a TS7016 somewhere our own dependency set
 * caused. README tells consumers to add `@types/ms` themselves.
 */
function isKnownUpstream(line: string): boolean {
  return line.includes('/node_modules/@temporalio/') && line.includes('error TS7016') && line.includes("'ms'");
}

const CONSUMER = `
import { XmemoryPlugin, XmemoryConfig, TYPE_BAD_OPTIONS } from '@xmemory/temporal';
import { xmemoryForWorkflow, TYPE_WRITE_TIMEOUT, type WriteMutation } from '@xmemory/temporal/workflow';

export const config: XmemoryConfig = { instanceId: 'inst-1' };
export const plugin = new XmemoryPlugin(config);
export const failureTypes: string[] = [TYPE_BAD_OPTIONS, TYPE_WRITE_TIMEOUT];
export async function readIt(): Promise<unknown> {
  return (await xmemoryForWorkflow().read('q')).readerResult;
}
// Structured mutations are the client's own type, reached through this package.
// It travels through our declarations, so a packaging regression upstream fails here.
export const mutations: WriteMutation[] = [
  { object_mutation: { object_type: 'Person', update: { key: { name: 'Ada' }, values: { role: 'eng' } } } },
];
export async function writeIt(): Promise<string> {
  return (await xmemoryForWorkflow().write('', { structuredMutations: mutations })).writeId;
}
// Text is required on every write: a default value makes it optional in the emitted
// declaration, and the call then enqueues an empty write. Structured-mutation
// callers pass '' and mean it.
// @ts-expect-error - writeDurable requires text
export const noDurableText = xmemoryForWorkflow().writeDurable();
// @ts-expect-error - write requires text
export const noWriteText = xmemoryForWorkflow().write();
// @ts-expect-error - writeAsyncStart requires text
export const noStartText = xmemoryForWorkflow().writeAsyncStart();
`;

/**
 * The package tarball to test: the one handed to us, or a fresh pack.
 *
 * On the release path the artifact that will actually be published is passed in
 * through `XMEMORY_TARBALL`, so the gates inspect those exact bytes rather than a
 * second build of the same source.
 */
function tarballPath(root: string, packDir: string): string {
  const given = process.env.XMEMORY_TARBALL;
  if (given) {
    console.log(`using the prebuilt tarball ${given}`);
    return resolve(given);
  }
  const packed = execFileSync('npm', ['pack', '--silent', '--pack-destination', packDir], {
    encoding: 'utf8',
    cwd: root,
  }).trim();
  return join(packDir, packed);
}

async function main(): Promise<void> {
  const root = process.cwd();
  const packDir = await mkdtemp(join(tmpdir(), 'xmemory-dts-pack-'));
  let failures = 0;
  try {
    const tarball = tarballPath(root, packDir);
    for (const major of NODE_TYPE_MAJORS) {
      const dir = await mkdtemp(join(tmpdir(), `xmemory-dts-node${major}-`));
      try {
      await writeFile(
        join(dir, 'package.json'),
        JSON.stringify({
          name: 'declaration-probe',
          private: true,
          dependencies: {
            '@xmemory/temporal': `file:${tarball}`,
            '@types/node': `^${major}`,
          },
        }),
      );
      // --ignore-scripts: this runs on the release path, so no dependency's install
      // hook gets to touch the workspace that is about to be packed and published.
      execFileSync('npm', ['install', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund', '--silent'], {
        cwd: dir,
        stdio: 'inherit',
      });
      await mkdir(join(dir, 'src'), { recursive: true });
      await writeFile(join(dir, 'src', 'main.ts'), CONSUMER);
      // The same consumer as an ES module. Under node16/nodenext a `.ts` in a
      // package without `"type": "module"` is CommonJS, so both lanes were testing
      // the same resolution; `.mts` is ESM regardless of the package.
      await writeFile(join(dir, 'src', 'main.mts'), CONSUMER);

      for (const mode of MODES) {
        await writeFile(
          join(dir, 'tsconfig.json'),
          JSON.stringify({
            compilerOptions: {
              target: 'ES2022',
              module: mode === 'bundler' ? 'esnext' : mode,
              moduleResolution: mode,
              strict: true,
              noEmit: true,
              skipLibCheck: false,
              types: ['node'],
            },
            include: ['src/**/*.ts', 'src/**/*.mts'],
          }),
        );
        let output = '';
        let failed = false;
        try {
          execFileSync(join(root, 'node_modules', '.bin', 'tsc'), ['-p', join(dir, 'tsconfig.json')], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
          });
        } catch (err) {
          const e = err as { stdout?: string; stderr?: string };
          output = `${e.stdout ?? ''}${e.stderr ?? ''}`;
          failed = true;
        }
        const diagnostics = output.split('\n').filter((line) => line.includes('error TS'));
        const blocking = diagnostics.filter((line) => !isKnownUpstream(line));
        const allowed = diagnostics.filter(isKnownUpstream);
        const label = `@types/node ${major}, ${mode}`;
        if (blocking.length > 0) {
          failures += 1;
          console.error(`declarations FAILED (${label}): ${blocking.length} error(s) a consumer would hit`);
          for (const line of blocking) console.error(`  ${line.trim()}`);
        } else if (failed && diagnostics.length === 0) {
          // tsc failed without a diagnostic we could parse — a crash, a bad config,
          // a changed output format. Never treat that as a pass.
          failures += 1;
          console.error(`declarations FAILED (${label}): tsc failed without a parseable diagnostic`);
          console.error(output.trim() || '  (no output)');
        } else {
          console.log(`declarations: ok (${label})`);
        }
        // Allowlisted, but never silent.
        for (const line of allowed) console.log(`  note, known upstream gap: ${line.trim()}`);
      }
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    }
  } finally {
    await rm(packDir, { recursive: true, force: true });
  }
  if (failures > 0) process.exit(1);
}

main().catch((err: unknown) => {
  console.error('declaration check FAILED:', err instanceof Error ? err.message : err);
  process.exit(1);
});
