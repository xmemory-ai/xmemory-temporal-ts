/**
 * Create an xmemory instance with a stable-PK schema for the example workflow.
 *
 * Run this ONCE, before the worker:
 *
 *   XMEM_API_KEY=xmem_...  npx tsx examples/setup-memory.ts
 *
 * It prints the new instance id — export it for the worker:
 *
 *   export XMEM_INSTANCE_ID="$(npx tsx examples/setup-memory.ts)"
 *
 * The schema keys people by their `name`, so the example's recall finds the
 * data (without it, recall comes back "not tracked"). NOTE: a name is an
 * LLM-normalized key, which can fork on re-extraction — so writes stay
 * at-most-once (the default); do not opt into write retries for a name-keyed
 * schema. See the README's idempotency section.
 */
import { SchemaType, XmemoryClient } from 'xmemory';

async function main(): Promise<void> {
  const url = process.env.XMEM_API_URL;
  // The API key is a bearer token, so plaintext must not carry it off-box. Compare
  // the parsed host: `http://localhost.evil.example` passes a prefix test.
  if (url !== undefined) {
    const { protocol, hostname } = new URL(url);
    if (protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(hostname)) {
      throw new Error('XMEM_API_URL must use https; plaintext is accepted only for loopback');
    }
  }
  const client = new XmemoryClient({ apiKey: process.env.XMEM_API_KEY, ...(url !== undefined ? { url } : {}) });

  const clusters = await client.admin.listClusters();
  if (clusters.length === 0) throw new Error('no xmemory cluster is available for this account');
  const clusterId = clusters[0].id;

  // The primary-key instruction is what makes repeated writes about a person
  // idempotent (a retry updates the same record).
  const schema = await client.admin.generateSchema(
    clusterId,
    'Track people the agent talks to. A person is identified by their full name. ' +
      'Record facts and preferences stated about each person — for example a preferred ' +
      'contact channel or role. Make name the primary key so repeated writes about the ' +
      'same person update the same record.',
  );

  const instance = await client.admin.createInstance(
    clusterId,
    'temporal-agent-memory',
    JSON.stringify(schema.data_schema),
    SchemaType.JSON,
  );
  console.log(instance.id);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
