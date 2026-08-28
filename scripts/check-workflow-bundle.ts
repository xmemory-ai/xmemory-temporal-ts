import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { bundleWorkflowCode } from '@temporalio/worker';

/**
 * Bundle a workflow that imports this package the way a consumer does.
 *
 * The probe runs from a scratch package OUTSIDE this checkout. That is the whole
 * point: inside our own package, Node's self-reference resolves `@xmemory/temporal`
 * to the working tree through its own `exports` map, so a tarball missing `dist`
 * entirely would still bundle. From a foreign package root the only thing that can
 * satisfy the import is the extracted artifact.
 */
async function main(): Promise<void> {
  const root = process.cwd();
  const dir = await mkdtemp(join(tmpdir(), 'xmemory-bundle-probe-'));
  try {
    // A different package name, so self-reference cannot apply here either.
    await writeFile(join(dir, 'package.json'), JSON.stringify({ name: 'workflow-bundle-probe', private: true }));

    // The artifact that will actually be published when one is handed to us
    // (the release path sets XMEMORY_TARBALL), so this inspects those exact bytes
    // rather than a second build of the same source.
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
    const installed = join(dir, 'node_modules', '@xmemory', 'temporal');
    await mkdir(installed, { recursive: true });
    execFileSync('tar', ['-xzf', tarball, '-C', installed, '--strip-components=1']);

    // Runtime dependencies come from the checkout, linked rather than reinstalled;
    // only our own package comes from the tarball.
    //
    // What a *consumer* would have installed: this package's declared production
    // dependencies, plus its peers, which they install themselves. Not everything in
    // node_modules — linking the whole directory hands the probe our
    // devDependencies too, so an import this package forgot to declare would
    // resolve here and fail for a real consumer.
    const closure = (args: string[]): string[] =>
      execFileSync('npm', ['ls', ...args, '--all', '--parseable'], { encoding: 'utf8', cwd: root })
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.includes('/node_modules/'));
    const manifest = JSON.parse(
      execFileSync('node', ['-p', 'JSON.stringify(require("./package.json"))'], { encoding: 'utf8', cwd: root }),
    ) as { peerDependencies?: Record<string, string> };
    const peers = Object.keys(manifest.peerDependencies ?? {});
    // The peers' own closures come with them: `npm ls <name>` walks each tree.
    const prod = [...new Set([...closure(['--omit=dev']), ...(peers.length > 0 ? closure(peers) : [])])];
    for (const path of prod) {
      // The package's name as it must resolve, scope included.
      const name = path.slice(path.lastIndexOf('/node_modules/') + '/node_modules/'.length);
      if (name.startsWith('@xmemory/')) continue;
      const target = join(dir, 'node_modules', name);
      await mkdir(dirname(target), { recursive: true });
      await symlink(path, target, 'dir').catch(() => {});
    }

    const workflows = join(dir, 'workflows.js');
    await writeFile(
      workflows,
      // By package name, through the `exports` map, which is what a consumer
      // resolves. Importing dist/workflow.js by path would pass even if the
      // subpath export were missing or misdeclared.
      "import { xmemoryForWorkflow } from '@xmemory/temporal/workflow';\n" +
        'export async function probe() {\n' +
        "  return (await xmemoryForWorkflow().read('q')).readerResult;\n" +
        '}\n',
    );
    await bundleWorkflowCode({ workflowsPath: workflows });
    console.log('workflow bundle: ok (packed artifact, resolved from a foreign package root)');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

main().catch((err: unknown) => {
  console.error('workflow bundle FAILED:', err);
  process.exit(1);
});
