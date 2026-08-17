/*
----
IDENTITY REPLAY
translates an original-ID-merge event stream (raw /export shape) into a
simplified-ID-merge event stream; see plans/original-to-simplified/design-draft.md
----
*/

const { Transform } = require('stream');
const fs = require('fs');
const path = require('path');
const md5 = require('md5');
const { IdentityGraph, stripDevicePrefix } = require('./identity-graph.js');
const { logger } = require('./logs.js');

/** @typedef {import('./job')} JobConfig */

const VERB_KINDS = {
	"$identify": "identify",
	"$create_alias": "alias",
	"$merge": "merge"
};

const DAY_IN_SECONDS = 86400;

/**
 * normalize + validate raw identityReplay options into canonical opts
 * compiles isUserId (fn | RegExp | regex string) into a predicate function
 * @param {Object} raw - the user-supplied identityReplay option group
 * @returns {Object} canonical opts
 */
function normalizeOptions(raw) {
	if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
		throw new Error(`identityReplay: expected an options object; got ${Array.isArray(raw) ? 'array' : typeof raw}`);
	}

	// isUserId is REQUIRED: fn | RegExp | regex string
	const { isUserId } = raw;
	if (isUserId === null || isUserId === undefined) {
		throw new Error(`identityReplay: isUserId is required — pass a function (id) => boolean, a RegExp, or a regex string that matches YOUR user ids`);
	}
	let predicate;
	if (typeof isUserId === 'function') {
		predicate = isUserId;
	}
	else if (isUserId instanceof RegExp) {
		// strip sticky/global flags so .test() has no lastIndex statefulness
		const re = new RegExp(isUserId.source, isUserId.flags.replace(/[gy]/g, ''));
		predicate = (id) => re.test(id);
	}
	else if (typeof isUserId === 'string') {
		let re;
		try {
			re = new RegExp(isUserId);
		}
		catch (e) {
			throw new Error(`identityReplay: isUserId string is not a valid regex: ${e.message}`);
		}
		predicate = (id) => re.test(id);
	}
	else {
		throw new Error(`identityReplay: isUserId must be a function, RegExp, or regex string; got ${typeof isUserId}`);
	}

	// enums
	const identityEvents = raw.identityEvents ?? 'rewrite';
	if (!['rewrite', 'drop'].includes(identityEvents)) {
		throw new Error(`identityReplay: identityEvents must be 'rewrite' or 'drop'; got '${identityEvents}'`);
	}
	const associationTimestamp = raw.associationTimestamp ?? 'original';
	if (!['original', 'floor'].includes(associationTimestamp)) {
		throw new Error(`identityReplay: associationTimestamp must be 'original' or 'floor'; got '${associationTimestamp}'`);
	}
	const bareDistinctId = raw.bareDistinctId ?? 'validate';
	if (!['validate', 'passthru'].includes(bareDistinctId)) {
		throw new Error(`identityReplay: bareDistinctId must be 'validate' or 'passthru'; got '${bareDistinctId}'`);
	}
	const onAmbiguous = raw.onAmbiguous ?? 'drop';
	if (!['drop', 'resolve', 'error'].includes(onAmbiguous)) {
		throw new Error(`identityReplay: onAmbiguous must be 'drop', 'resolve', or 'error'; got '${onAmbiguous}'`);
	}
	const onGraphOverflow = raw.onGraphOverflow ?? 'warn';
	if (!['warn', 'abort'].includes(onGraphOverflow)) {
		throw new Error(`identityReplay: onGraphOverflow must be 'warn' or 'abort'; got '${onGraphOverflow}'`);
	}

	// numbers
	const maxGraphSize = raw.maxGraphSize ?? 5_000_000;
	if (!Number.isFinite(maxGraphSize) || maxGraphSize <= 0) {
		throw new Error(`identityReplay: maxGraphSize must be a positive number; got ${maxGraphSize}`);
	}
	const minAssociationRate = raw.minAssociationRate ?? 0;
	if (!Number.isFinite(minAssociationRate) || minAssociationRate < 0 || minAssociationRate > 1) {
		throw new Error(`identityReplay: minAssociationRate must be between 0 and 1; got ${minAssociationRate}`);
	}

	// collections
	let userIdFallbackProps = raw.userIdFallbackProps ?? [];
	if (typeof userIdFallbackProps === 'string') userIdFallbackProps = [userIdFallbackProps];
	if (!Array.isArray(userIdFallbackProps)) {
		throw new Error(`identityReplay: userIdFallbackProps must be an array of property names`);
	}
	let denylistRaw = raw.denylist ?? [];
	if (typeof denylistRaw === 'string') denylistRaw = [denylistRaw];
	if (!Array.isArray(denylistRaw) && !(denylistRaw instanceof Set)) {
		throw new Error(`identityReplay: denylist must be an array (or Set) of ids`);
	}
	// denylist entries are stored $device:-stripped so both representations match
	const denylist = new Set([...denylistRaw].map((id) => stripDevicePrefix(String(id))));

	const associationProps = raw.associationProps ?? {};
	if (!associationProps || typeof associationProps !== 'object' || Array.isArray(associationProps)) {
		throw new Error(`identityReplay: associationProps must be a plain object of static props`);
	}

	return {
		isUserId: predicate,
		graph: raw.graph ?? true,
		maxGraphSize,
		onGraphOverflow,
		identityEvents,
		associationEventName: raw.associationEventName || 'identity association',
		associationTimestamp,
		associationProps,
		bareDistinctId,
		userIdFallbackProps,
		denylist,
		onAmbiguous,
		minAssociationRate,
		graphPath: raw.graphPath || ''
	};
}

/**
 * the record's property bag: nested {event, properties} or flat /export shape
 * @param {Object} record
 * @returns {Object}
 */
function getProps(record) {
	if (record && typeof record === 'object' && record.properties && typeof record.properties === 'object') {
		return record.properties;
	}
	return record;
}

/**
 * the as-ingested id of an ordinary row: $distinct_id_before_identity ?? distinct_id
 * (never trust exported distinct_id as "the user" — canonical can be an anon uuid)
 * @param {Object} props
 * @returns {string | null}
 */
function getAsIngestedId(props) {
	const id = props.$distinct_id_before_identity ?? props.distinct_id;
	if (id === null || id === undefined || id === '') return null;
	return String(id);
}

/**
 * classify a record: verb detection + evidence (edge) extraction
 * ranks: 0 = hard dual-ID row, 1 = verb edge, 2 = inferred/fallback-prop
 * every id is $device:-stripped before it becomes evidence
 * @param {Object} record
 * @param {Object} opts - canonical opts from normalizeOptions
 * @returns {{kind: 'identify'|'alias'|'merge'|'event', edges: Array<[string, string, number]>, rewrite: boolean, denylisted: number}}
 */
function classifyRecord(record, opts) {
	const props = getProps(record);
	const kind = VERB_KINDS[record.event] || 'event';
	const edges = [];
	let denylisted = 0;

	const addEdge = (a, b, rank) => {
		if (a === null || a === undefined || b === null || b === undefined) return;
		a = stripDevicePrefix(String(a));
		b = stripDevicePrefix(String(b));
		if (!a || !b || a === b) return;
		if (opts.denylist.has(a) || opts.denylist.has(b)) {
			denylisted++;
			return;
		}
		edges.push([a, b, rank]);
	};

	if (kind === 'identify') {
		addEdge(props.$identified_id, props.$anon_id, 1);
	}
	else if (kind === 'alias') {
		// distinct_id-side resolved before_identity-aware
		addEdge(props.$distinct_id_before_identity ?? props.distinct_id, props.alias, 1);
	}
	else if (kind === 'merge') {
		const ids = props.$distinct_ids;
		if (Array.isArray(ids) && ids.length === 2) {
			addEdge(ids[0], ids[1], 1);
		}
	}
	else {
		// ordinary event
		const hasUser = props.$user_id !== null && props.$user_id !== undefined && props.$user_id !== '';
		const hasDevice = props.$device_id !== null && props.$device_id !== undefined && props.$device_id !== '';
		if (hasUser && hasDevice) {
			addEdge(props.$user_id, props.$device_id, 0);
		}
		else if (!hasUser && !hasDevice) {
			// bare event: no edge except via userIdFallbackProps
			const asIngested = getAsIngestedId(props);
			if (asIngested !== null) {
				const stripped = stripDevicePrefix(asIngested);
				for (const prop of opts.userIdFallbackProps) {
					const val = props[prop];
					if (val === null || val === undefined || val === '') continue;
					const candidate = String(val);
					if (candidate === asIngested || candidate === stripped) continue; // must differ from as-ingested
					if (!opts.isUserId(candidate, record)) continue; // must pass isUserId
					addEdge(candidate, stripped, 2);
					break;
				}
			}
		}
	}

	return { kind, edges, rewrite: kind === 'event', denylisted };
}

/**
 * rewrite an ORDINARY event in place for simplified id-merge
 * returns the (mutated) record, or null when the record is denylisted (drop + count upstream)
 * @param {Object} record
 * @param {Object} opts - canonical opts from normalizeOptions
 * @returns {Object | null}
 */
function rewriteEvent(record, opts) {
	const props = getProps(record);
	const asIngested = getAsIngestedId(props);
	const stripped = asIngested !== null ? stripDevicePrefix(asIngested) : null;

	// denylisted ids: drop the whole record (caller counts)
	if (stripped !== null && opts.denylist.has(stripped)) return null;
	if (props.$user_id !== null && props.$user_id !== undefined && opts.denylist.has(stripDevicePrefix(String(props.$user_id)))) return null;
	if (props.$device_id !== null && props.$device_id !== undefined && opts.denylist.has(stripDevicePrefix(String(props.$device_id)))) return null;

	const hasUser = props.$user_id !== null && props.$user_id !== undefined && props.$user_id !== '';
	const hasDevice = props.$device_id !== null && props.$device_id !== undefined && props.$device_id !== '';

	// dual-ID (or single-ID-prop) rows: strip device prefix, ensure distinct_id consistency, done
	if (hasUser || hasDevice) {
		if (hasDevice) props.$device_id = stripDevicePrefix(String(props.$device_id));
		if (hasUser) {
			props.$user_id = String(props.$user_id);
			props.distinct_id = props.$user_id;
		}
		else {
			props.distinct_id = `$device:${props.$device_id}`;
		}
		props.$id_replay_source = 'dual-row';
		return record;
	}

	// bare distinct_id policy
	if (opts.bareDistinctId === 'passthru') return record;
	if (asIngested === null) return record; // nothing to classify

	const wasPrefixed = stripped !== asIngested;

	// unprefixed + passes isUserId → user
	if (!wasPrefixed && opts.isUserId(asIngested, record)) {
		props.$user_id = asIngested;
		props.distinct_id = asIngested;
		props.$id_replay_source = 'bare-user';
		return record;
	}

	// device side (already-prefixed ids are ALWAYS devices, even if the stripped value looks like a user)
	// probe fallback props for a user id — a passing, differing value makes this row a dual row
	let fallbackUser = null;
	for (const prop of opts.userIdFallbackProps) {
		const val = props[prop];
		if (val === null || val === undefined || val === '') continue;
		const candidate = String(val);
		if (candidate === asIngested || candidate === stripped) continue;
		if (!opts.isUserId(candidate, record)) continue;
		fallbackUser = candidate;
		break;
	}

	props.$device_id = stripped;
	if (fallbackUser !== null) {
		props.$user_id = fallbackUser;
		props.distinct_id = fallbackUser;
		props.$id_replay_source = 'fallback-prop';
	}
	else {
		props.distinct_id = `$device:${stripped}`; // strip repeats first; never double-prefix
		props.$id_replay_source = 'bare-device';
	}
	return record;
}

/**
 * build a doc-sanctioned dual-ID association event
 * deterministic $insert_id = md5(user + '|' + device) so re-runs + dupes self-dedupe at query time
 * @param {string} user
 * @param {string} device
 * @param {Object} opts - canonical opts from normalizeOptions
 * @param {{ts?: number, floorTs?: number, source?: 'verb'|'closure'}} [meta]
 * @returns {Object} nested {event, properties} record
 */
function buildAssociationEvent(user, device, opts, meta = {}) {
	const usr = String(user);
	const dev = stripDevicePrefix(String(device));
	const time = opts.associationTimestamp === 'floor' ? meta.floorTs : meta.ts;
	return {
		event: opts.associationEventName,
		properties: {
			$user_id: usr,
			$device_id: dev,
			distinct_id: usr,
			time,
			$insert_id: md5(`${usr}|${dev}`),
			$id_replay_source: meta.source,
			...opts.associationProps
		}
	};
}

/**
 * numeric time or null
 * @param {*} t
 * @returns {number | null}
 */
function toTs(t) {
	const n = Number(t);
	return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * write the graphPath JSONL artifact: one row per resolved pair + a trailer line
 * never throws — unwritable paths warn + set a telemetry flag; cloud paths are skipped in v1
 * @param {string} graphPath
 * @param {Array<Object>} rows
 * @param {Object} trailer
 * @param {Object} stats
 * @param {Function} log
 */
function writeGraphArtifact(graphPath, rows, trailer, stats, log) {
	if (/^(gs|s3):\/\//i.test(graphPath)) {
		// cloud graphPath is a v1 SKIP (no trivial sync write path); counted + warned
		stats.graphPathSkipped = true;
		log(`identityReplay: graphPath '${graphPath}' is a cloud path — skipped in v1; use a local path`);
		return;
	}
	try {
		const resolved = path.resolve(graphPath);
		fs.mkdirSync(path.dirname(resolved), { recursive: true });
		const lines = rows.map((r) => JSON.stringify(r));
		lines.push(JSON.stringify(trailer));
		fs.writeFileSync(resolved, lines.join('\n') + '\n');
		stats.graphPathWritten = resolved;
	}
	catch (err) {
		stats.graphPathError = err.message;
		log(`identityReplay: could not write graphPath '${graphPath}': ${err.message}`);
	}
}

/**
 * the pipeline stage: an objectMode Transform
 * verbs → absorbed into the graph, never forwarded; ordinary events → rewritten + pushed 1:1;
 * _flush → transitive-closure association events per resolved cluster + telemetry onto the job
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createIdentityReplay(job) {
	const opts = normalizeOptions(job.identityReplay);
	const log = logger(job);

	const stats = {
		verbsSeen: { identify: 0, alias: 0, merge: 0 },
		assocEmitted: { live: 0, closure: 0 },
		bare: { user: 0, device: 0, prefixedAlready: 0 },
		denylisted: 0,
		ambiguous: { merges: 0, clusters: 0 },
		clusters: { total: 0, resolved: 0, anonOnly: 0, multiUser: 0 },
		unresolvedAnonIds: 0,
		deferredImpossible: 0,
		malformedVerbs: 0,
		graphOverflowEdges: 0,
		isUserIdPassRate: 0,
		associationRate: 0
	};

	// wrap the predicate for pass-rate telemetry
	let isUserIdCalls = 0;
	let isUserIdPasses = 0;
	const rawPredicate = opts.isUserId;
	opts.isUserId = (id, record) => {
		isUserIdCalls++;
		const pass = Boolean(rawPredicate(id, record));
		if (pass) isUserIdPasses++;
		return pass;
	};

	/** @type {InstanceType<typeof IdentityGraph> | null} */
	const graph = opts.graph ? new IdentityGraph({ maxNodes: opts.maxGraphSize }) : null;
	/** @type {Map<string, {firstTs: number | null, rank: number}> | null} id → earliest ts + best evidence rank */
	const nodeMeta = graph ? new Map() : null;
	/** @type {Map<string, Set<string>> | null} device → users it was DIRECTLY verb/row-paired with */
	const directPairs = graph ? new Map() : null;
	let minEventTime = Infinity;

	const trackMeta = (id, ts, rank) => {
		const existing = nodeMeta.get(id);
		if (!existing) {
			nodeMeta.set(id, { firstTs: ts, rank });
			return;
		}
		if (ts !== null && (existing.firstTs === null || ts < existing.firstTs)) existing.firstTs = ts;
		if (rank !== null && (existing.rank === null || rank < existing.rank)) existing.rank = rank;
	};

	const finalizeStats = () => {
		if (graph) stats.graphOverflowEdges = graph.overflowEdges;
		const verbsTotal = stats.verbsSeen.identify + stats.verbsSeen.alias + stats.verbsSeen.merge;
		const emitted = stats.assocEmitted.live + stats.assocEmitted.closure;
		stats.isUserIdPassRate = isUserIdCalls > 0 ? isUserIdPasses / isUserIdCalls : 0;
		stats.associationRate = emitted / Math.max(1, verbsTotal);
		job.identityReplayStats = stats;
	};

	const stream = new Transform({
		objectMode: true,
		highWaterMark: job.highWater || 16,

		transform(record, encoding, callback) {
			try {
				if (!record || typeof record !== 'object') return callback();
				const { kind, edges, denylisted } = classifyRecord(record, opts);
				stats.denylisted += denylisted;
				const props = getProps(record);
				const ts = toTs(props.time);
				if (ts !== null && ts < minEventTime) minEventTime = ts;

				// harvest edges into the graph
				if (graph) {
					for (const [a, b, rank] of edges) {
						trackMeta(a, ts, rank);
						trackMeta(b, ts, rank);
						const aIsUser = opts.isUserId(a, record);
						const bIsUser = opts.isUserId(b, record);
						const aOk = graph.addNode(a, { isUser: aIsUser, ts, rank });
						const bOk = graph.addNode(b, { isUser: bIsUser, ts, rank });
						if (aOk && bOk) graph.addEdge(a, b);
						// remember direct device↔user pairings (verb vs closure provenance at flush)
						if (aIsUser !== bIsUser) {
							const device = aIsUser ? b : a;
							const user = aIsUser ? a : b;
							if (!directPairs.has(device)) directPairs.set(device, new Set());
							directPairs.get(device).add(user);
						}
					}
					if (opts.onGraphOverflow === 'abort' && graph.overflowEdges > 0) {
						finalizeStats();
						return callback(new Error(`identityReplay: identity graph hit maxGraphSize=${opts.maxGraphSize} (${graph.overflowEdges} edges dropped) and onGraphOverflow='abort'`));
					}
				}

				// identity verbs: swallow (never forward — simplified /import hard-rejects them)
				if (kind !== 'event') {
					stats.verbsSeen[kind]++;
					if (kind === 'merge' && !Array.isArray(props.$distinct_ids)) stats.malformedVerbs++;
					else if (kind === 'merge' && Array.isArray(props.$distinct_ids) && props.$distinct_ids.length !== 2) stats.malformedVerbs++;

					// lite mode (graph:false): stateless 1:1 verb rewrite when exactly one side is a user
					if (!graph && opts.identityEvents === 'rewrite' && edges.length === 1) {
						const [a, b] = edges[0];
						const aIsUser = opts.isUserId(a, record);
						const bIsUser = opts.isUserId(b, record);
						if (aIsUser !== bIsUser) {
							const user = aIsUser ? a : b;
							const device = aIsUser ? b : a;
							const now = Math.floor(Date.now() / 1000);
							const assoc = buildAssociationEvent(user, device, opts, {
								ts: ts ?? now,
								floorTs: (ts ?? now) - DAY_IN_SECONDS,
								source: 'verb'
							});
							stats.assocEmitted.live++;
							this.push(assoc);
						}
						else if (aIsUser && bIsUser) {
							stats.ambiguous.merges++; // two users; needs election → impossible statelessly
						}
						else {
							stats.deferredImpossible++; // two anons; needs closure → impossible statelessly
						}
					}
					return callback(); // verb absorbed
				}

				// ordinary event: rewrite + push 1:1
				const asIngested = getAsIngestedId(props);
				if (asIngested !== null && asIngested.startsWith('$device:')) stats.bare.prefixedAlready++;
				// a bare sighting counts toward a node's first-seen ts ('original' assoc timestamps),
				// even though it contributes no edge (rank stays null — a sighting is not evidence)
				if (graph && asIngested !== null && ts !== null) trackMeta(stripDevicePrefix(asIngested), ts, null);
				const rewritten = rewriteEvent(record, opts);
				if (rewritten === null) {
					stats.denylisted++;
					return callback(); // denylisted record dropped
				}
				const source = getProps(rewritten).$id_replay_source;
				if (source === 'bare-user') stats.bare.user++;
				else if (source === 'bare-device' || source === 'fallback-prop') stats.bare.device++;
				callback(null, rewritten);
			}
			catch (err) {
				callback(err);
			}
		},

		flush(callback) {
			try {
				const now = Math.floor(Date.now() / 1000);
				const minTs = Number.isFinite(minEventTime) ? minEventTime : now;
				const floorTs = minTs - DAY_IN_SECONDS;
				/** @type {Array<{device: string, user: string, rank: number|null, source: string}>} */
				const pairs = [];
				let unresolvedClusters = 0;

				if (graph) {
					for (const cluster of graph.clusters()) {
						stats.clusters.total++;
						const users = cluster.users;
						const userIds = new Set(users.map((u) => u.id));
						const anonMembers = cluster.members.filter((m) => !userIds.has(m));

						if (users.length === 0) {
							stats.clusters.anonOnly++;
							unresolvedClusters++;
							stats.unresolvedAnonIds += anonMembers.length;
							continue; // members stay $device:-anonymous (already correct from per-event rewrite)
						}

						let winner;
						if (users.length === 1) {
							winner = users[0].id;
						}
						else {
							stats.clusters.multiUser++;
							stats.ambiguous.clusters++;
							if (opts.onAmbiguous === 'error') {
								finalizeStats();
								return callback(new Error(`identityReplay: ambiguous cluster with ${users.length} users [${users.map((u) => u.id).join(', ')}] and onAmbiguous='error'`));
							}
							if (opts.onAmbiguous === 'drop') {
								unresolvedClusters++;
								stats.unresolvedAnonIds += anonMembers.length;
								continue; // anons in a multi-user cluster get NO assoc events
							}
							// 'resolve': elect winner by evidence rank → latest ts → lexicographic min
							const elected = graph.electUser(users, opts.onAmbiguous);
							winner = typeof elected === 'string' ? elected : elected.id;
						}
						stats.clusters.resolved++;

						// one assoc event per non-user member (losers stay their own identified users)
						for (const member of anonMembers) {
							const meta = nodeMeta.get(member) || {};
							const source = directPairs.has(member) && directPairs.get(member).has(winner) ? 'verb' : 'closure';
							pairs.push({ device: member, user: winner, rank: meta.rank ?? null, source });
							if (opts.identityEvents === 'rewrite') {
								this.push(buildAssociationEvent(winner, member, opts, {
									ts: meta.firstTs ?? minTs,
									floorTs,
									source
								}));
								if (source === 'verb') stats.assocEmitted.live++;
								else stats.assocEmitted.closure++;
							}
						}
					}
				}

				// graphPath artifact: resolved pair table + trailer (write only if writable)
				if (opts.graphPath) {
					writeGraphArtifact(
						opts.graphPath,
						pairs,
						{ unresolvedClusters, ambiguousClusters: stats.ambiguous.clusters },
						stats,
						log
					);
				}

				finalizeStats();

				// fail-closed coverage floor
				if (opts.identityEvents === 'rewrite' && opts.minAssociationRate > 0 && stats.associationRate < opts.minAssociationRate) {
					return callback(new Error(`identityReplay: association rate ${stats.associationRate.toFixed(4)} is below minAssociationRate ${opts.minAssociationRate}; telemetry: ${JSON.stringify(stats)}`));
				}

				callback();
			}
			catch (err) {
				try {
					finalizeStats();
				}
				catch (e) {
					// stats finalization must never mask the original error
				}
				callback(err);
			}
		}
	});

	// expose accumulated telemetry on the stage (job.identityReplayStats is assigned at flush)
	// @ts-ignore
	stream.stats = stats;
	return stream;
}

module.exports = {
	createIdentityReplay,
	// internals exported for unit tests
	normalizeOptions,
	classifyRecord,
	rewriteEvent,
	buildAssociationEvent
};
