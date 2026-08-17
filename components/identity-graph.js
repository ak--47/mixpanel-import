/*
----
IDENTITY GRAPH
----
Union-find (disjoint set) over string ids, used by the identityReplay feature to
compute the transitive closure of identity evidence ($identify / $create_alias /
$merge verbs, hard dual-ID rows, inferred fallback props) in a single streaming pass.

Design contract: plans/original-to-simplified/design-draft.md §"Pinned interfaces".

- Path compression + union by size ⇒ effectively O(α(n)) per op.
- Memory: one Map entry per distinct id (NOT per pair/edge).
- Per-node metadata: { isUser, rank, firstTs, lastTs }.
  Ranks: 0 = hard dual-ID row, 1 = verb edge, 2 = inferred/fallback-prop.
  Election prefers the LOWEST rank (strongest evidence).
- addNode is idempotent and UPGRADES metadata: a lower (stronger) rank wins,
  the ts range extends in both directions, isUser can only turn on.
- maxNodes cap: NEW ids are rejected once the cap is hit; already-known ids are
  always accepted (idempotent re-adds keep upgrading metadata at cap). Every
  addEdge that fails because a side is missing while the graph is at capacity
  increments `overflowEdges` (telemetry: graphOverflowEdges).
- Denylist handling is deliberately NOT here — the caller (identity-replay stage)
  filters denylisted ids before they ever reach the graph.

This module has no internal require()s and no process-global side effects.
*/

/** the reserved prefix Mixpanel uses to mark device-scoped distinct_ids */
const DEVICE_PREFIX = '$device:';

/**
 * @typedef {Object} NodeMeta
 * @property {boolean} isUser - whether the id passed the caller's isUserId predicate at insert
 * @property {number} rank - evidence rank: 0 = hard dual-ID row | 1 = verb edge | 2 = inferred/fallback
 * @property {number | null} firstTs - earliest timestamp seen for this id (epoch; unit is the caller's)
 * @property {number | null} lastTs - latest timestamp seen for this id
 */

/**
 * @typedef {Object} ClusterUser
 * @property {string} id - the user id
 * @property {number} rank - the node's (best) evidence rank
 * @property {number | null} ts - the node's lastTs (recency, used as the election tiebreak)
 */

/**
 * @typedef {Object} Cluster
 * @property {string[]} members - EVERY id in the cluster (single-member clusters included; caller filters)
 * @property {ClusterUser[]} users - the subset of members flagged isUser, with election inputs
 */

/**
 * strips ALL leading '$device:' prefixes from an id
 * (ingest normalizes one prefix — probe p08 — but we never rely on it; the Vipps
 * '$device:$device:' corruption is exactly what this guards against)
 * non-string-safe: returns the input untouched unless it is a string
 * @param {any} id - candidate id
 * @returns {any} the id with every leading '$device:' removed (strings only)
 */
function stripDevicePrefix(id) {
	if (typeof id !== 'string') return id;
	while (id.startsWith(DEVICE_PREFIX)) {
		id = id.slice(DEVICE_PREFIX.length);
	}
	return id;
}

/**
 * union-find identity graph with per-node evidence metadata
 */
class IdentityGraph {
	/**
	 * @param {Object} [opts]
	 * @param {number} [opts.maxNodes=5000000] - node-count cap; new ids are rejected at cap
	 */
	constructor(opts = {}) {
		const { maxNodes = 5_000_000 } = opts;

		/** @type {number} node-count cap */
		this.maxNodes = maxNodes;

		/** @type {Map<string, string>} id -> parent id (roots point at themselves) */
		this.parent = new Map();

		/** @type {Map<string, number>} root id -> cluster size (only maintained for roots) */
		this.clusterSizes = new Map();

		/** @type {Map<string, NodeMeta>} id -> metadata */
		this.meta = new Map();

		/** @type {number} edges skipped because a side was rejected at the node cap */
		this.overflowEdges = 0;

		/** @type {Map<string, ClusterUser[]> | null} memoized root -> users index; invalidated on mutation */
		this._userIndexCache = null;
	}

	/** @returns {number} count of distinct ids in the graph */
	get size() {
		return this.parent.size;
	}

	/** @returns {boolean} whether the graph has reached its node cap */
	get atCapacity() {
		return this.parent.size >= this.maxNodes;
	}

	/**
	 * adds (or upgrades) a node; idempotent
	 * - existing ids are ALWAYS accepted (even at cap) and their metadata is upgraded:
	 *   lower rank wins, firstTs/lastTs range extends, isUser can only turn on
	 * - new ids are rejected once the graph is at capacity
	 * @param {string} id - the id (caller strips '$device:' and applies the denylist first)
	 * @param {Object} [nodeMeta]
	 * @param {boolean} [nodeMeta.isUser=false] - result of the caller's isUserId predicate
	 * @param {number} [nodeMeta.ts] - a timestamp associated with this sighting
	 * @param {number} [nodeMeta.rank=2] - evidence rank of this sighting (0 hard | 1 verb | 2 inferred)
	 * @returns {boolean} true if the node exists in the graph after the call; false if rejected
	 */
	addNode(id, nodeMeta = {}) {
		if (typeof id !== 'string' || id === '') return false;
		const { isUser = false, ts, rank = 2 } = nodeMeta;
		const hasTs = typeof ts === 'number' && !Number.isNaN(ts);

		const existing = this.meta.get(id);
		if (existing) {
			// upgrade-only merge
			if (rank < existing.rank) existing.rank = rank;
			if (isUser && !existing.isUser) existing.isUser = true;
			if (hasTs) {
				if (existing.firstTs === null || ts < existing.firstTs) existing.firstTs = ts;
				if (existing.lastTs === null || ts > existing.lastTs) existing.lastTs = ts;
			}
			this._userIndexCache = null;
			return true;
		}

		// new node: reject at cap
		if (this.atCapacity) return false;

		this.parent.set(id, id);
		this.clusterSizes.set(id, 1);
		this.meta.set(id, {
			isUser,
			rank,
			firstTs: hasTs ? ts : null,
			lastTs: hasTs ? ts : null
		});
		this._userIndexCache = null;
		return true;
	}

	/**
	 * reads a node's metadata (a copy; the graph's internals stay private)
	 * @param {string} id
	 * @returns {NodeMeta | null} metadata, or null if the id is not in the graph
	 */
	getNode(id) {
		const meta = this.meta.get(id);
		if (!meta) return null;
		return { ...meta };
	}

	/**
	 * unions the clusters containing a and b (nodes must have been added first)
	 * @param {string} a - one side of the edge
	 * @param {string} b - the other side
	 * @returns {boolean} true if the union happened (or was already in effect);
	 * false if either side is missing — and when the graph is at capacity, a missing
	 * side is presumed cap-rejected and the edge is counted in `overflowEdges`
	 */
	addEdge(a, b) {
		if (!this.parent.has(a) || !this.parent.has(b)) {
			if (this.atCapacity) this.overflowEdges++;
			return false;
		}

		let rootA = this._find(a);
		let rootB = this._find(b);
		if (rootA === rootB) return true; // already unioned; still success

		// union by size: the smaller tree hangs off the larger
		const sizeA = this.clusterSizes.get(rootA);
		const sizeB = this.clusterSizes.get(rootB);
		if (sizeA < sizeB) {
			const swap = rootA;
			rootA = rootB;
			rootB = swap;
		}
		this.parent.set(rootB, rootA);
		this.clusterSizes.set(rootA, sizeA + sizeB);
		this.clusterSizes.delete(rootB);
		this._userIndexCache = null;
		return true;
	}

	/**
	 * finds the root of an id with full path compression (iterative, two-pass)
	 * @param {string} id - MUST be present in the graph
	 * @returns {string} the root id
	 */
	_find(id) {
		let root = id;
		while (this.parent.get(root) !== root) {
			root = this.parent.get(root);
		}
		// second pass: point every node on the path directly at the root
		let current = id;
		while (current !== root) {
			const next = this.parent.get(current);
			this.parent.set(current, root);
			current = next;
		}
		return root;
	}

	/**
	 * builds (and memoizes) the root -> users index; invalidated on any mutation
	 * @returns {Map<string, ClusterUser[]>}
	 */
	_userIndex() {
		if (this._userIndexCache) return this._userIndexCache;
		const index = new Map();
		for (const [id, meta] of this.meta) {
			if (!meta.isUser) continue;
			const root = this._find(id);
			const users = index.get(root);
			const entry = { id, rank: meta.rank, ts: meta.lastTs };
			if (users) users.push(entry);
			else index.set(root, [entry]);
		}
		this._userIndexCache = index;
		return index;
	}

	/**
	 * materializes EVERY cluster — multi-member AND single-member — in one pass
	 * over the union-find; the caller filters (e.g. skips singletons, splits
	 * anon-only vs resolved vs multi-user)
	 * @returns {Cluster[]}
	 */
	clusters() {
		const byRoot = new Map();
		for (const id of this.parent.keys()) {
			const root = this._find(id);
			let cluster = byRoot.get(root);
			if (!cluster) {
				cluster = { members: [], users: [] };
				byRoot.set(root, cluster);
			}
			cluster.members.push(id);
			const meta = this.meta.get(id);
			if (meta.isUser) cluster.users.push({ id, rank: meta.rank, ts: meta.lastTs });
		}
		return Array.from(byRoot.values());
	}

	/**
	 * resolves an id to its cluster's elected canonical user id (for the graphPath
	 * artifact and tests)
	 * @param {string} id - any id in the graph
	 * @param {string} [mode='resolve'] - election mode, see {@link IdentityGraph.electUser}
	 * @returns {string | null} the elected user id, or null (unknown id, anon-only
	 * cluster, or multi-user cluster under mode 'drop')
	 */
	resolve(id, mode = 'resolve') {
		if (typeof id !== 'string' || !this.parent.has(id)) return null;
		const root = this._find(id);
		const users = this._userIndex().get(root) || [];
		return IdentityGraph.electUser(users, mode);
	}

	/**
	 * instance convenience wrapper for {@link IdentityGraph.electUser}
	 * @param {ClusterUser[]} users
	 * @param {string} [mode]
	 * @returns {string | null}
	 */
	electUser(users, mode) {
		return IdentityGraph.electUser(users, mode);
	}

	/**
	 * elects the canonical user from a cluster's user list
	 * ordering: rank ASC (0 hard beats 1 verb beats 2 inferred)
	 *   → lastTs DESC (most recent wins)
	 *   → id ASC (lexicographic min; the deterministic tiebreak)
	 * modes (per onAmbiguous):
	 * - 'resolve' (default): multi-user clusters elect a winner by the ordering above
	 * - 'drop': multi-user clusters elect NO ONE (returns null); single-user clusters
	 *   still return their user
	 * - 'error' is the CALLER's job (it can see users.length > 1 itself and abort);
	 *   any unrecognized mode behaves like 'resolve'
	 * @param {ClusterUser[]} users - the cluster's users ({ id, rank, ts })
	 * @param {string} [mode='resolve'] - 'resolve' | 'drop'
	 * @returns {string | null} the elected user id or null
	 */
	static electUser(users, mode = 'resolve') {
		if (!Array.isArray(users) || users.length === 0) return null;
		if (users.length === 1) return users[0].id;
		if (mode === 'drop') return null;

		let winner = users[0];
		for (let i = 1; i < users.length; i++) {
			const candidate = users[i];
			if (IdentityGraph._beats(candidate, winner)) winner = candidate;
		}
		return winner.id;
	}

	/**
	 * comparator core: does candidate beat incumbent in the election?
	 * @param {ClusterUser} candidate
	 * @param {ClusterUser} incumbent
	 * @returns {boolean}
	 */
	static _beats(candidate, incumbent) {
		if (candidate.rank !== incumbent.rank) return candidate.rank < incumbent.rank; // rank asc
		const candidateTs = candidate.ts ?? Number.NEGATIVE_INFINITY;
		const incumbentTs = incumbent.ts ?? Number.NEGATIVE_INFINITY;
		if (candidateTs !== incumbentTs) return candidateTs > incumbentTs; // lastTs desc
		return candidate.id < incumbent.id; // id asc (lexicographic)
	}
}

module.exports = { IdentityGraph, stripDevicePrefix };
