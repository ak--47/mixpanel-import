// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* cSpell:disable */

/*
Unit tests for components/identity-graph.js — the union-find identity graph behind
the identityReplay feature. Build contract:
plans/original-to-simplified/design-draft.md §"Pinned interfaces".
*/

const { IdentityGraph, stripDevicePrefix } = require("../components/identity-graph.js");

describe("stripDevicePrefix", () => {
	test("no prefix: returns the id unchanged", () => {
		expect(stripDevicePrefix("abc-123")).toBe("abc-123");
	});

	test("single '$device:' prefix is stripped", () => {
		expect(stripDevicePrefix("$device:abc-123")).toBe("abc-123");
	});

	test("double '$device:$device:' prefix is fully stripped (Vipps corruption case)", () => {
		expect(stripDevicePrefix("$device:$device:abc-123")).toBe("abc-123");
	});

	test("many repeated prefixes are all stripped", () => {
		expect(stripDevicePrefix("$device:".repeat(5) + "uuid")).toBe("uuid");
	});

	test("exactly '$device:' strips to empty string", () => {
		expect(stripDevicePrefix("$device:")).toBe("");
	});

	test("'$device:' in the MIDDLE is not touched (leading only)", () => {
		expect(stripDevicePrefix("abc$device:def")).toBe("abc$device:def");
	});

	test("non-string inputs are returned untouched", () => {
		expect(stripDevicePrefix(42)).toBe(42);
		expect(stripDevicePrefix(null)).toBe(null);
		expect(stripDevicePrefix(undefined)).toBe(undefined);
		const obj = { $device: true };
		expect(stripDevicePrefix(obj)).toBe(obj);
		const arr = ["$device:abc"];
		expect(stripDevicePrefix(arr)).toBe(arr);
		expect(stripDevicePrefix(true)).toBe(true);
	});

	test("empty string is returned as-is", () => {
		expect(stripDevicePrefix("")).toBe("");
	});
});

describe("addNode", () => {
	test("adding a node grows size and returns true", () => {
		const graph = new IdentityGraph();
		expect(graph.size).toBe(0);
		expect(graph.addNode("a", { isUser: false, ts: 100, rank: 1 })).toBe(true);
		expect(graph.size).toBe(1);
	});

	test("is idempotent: re-adding the same id does not grow size", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { isUser: false, ts: 100, rank: 1 });
		expect(graph.addNode("a", { isUser: false, ts: 100, rank: 1 })).toBe(true);
		expect(graph.addNode("a")).toBe(true);
		expect(graph.size).toBe(1);
	});

	test("defaults: rank 2 (inferred), isUser false, null ts range", () => {
		const graph = new IdentityGraph();
		graph.addNode("a");
		expect(graph.getNode("a")).toEqual({ isUser: false, rank: 2, firstTs: null, lastTs: null });
	});

	test("metadata upgrade: LOWER rank wins, higher rank never downgrades", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { rank: 2 });
		graph.addNode("a", { rank: 0 }); // hard dual-ID row: upgrade
		expect(graph.getNode("a").rank).toBe(0);
		graph.addNode("a", { rank: 2 }); // weaker evidence later: no downgrade
		expect(graph.getNode("a").rank).toBe(0);
	});

	test("metadata upgrade: ts range EXTENDS in both directions", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { ts: 100 });
		expect(graph.getNode("a").firstTs).toBe(100);
		expect(graph.getNode("a").lastTs).toBe(100);
		graph.addNode("a", { ts: 50 }); // earlier sighting
		graph.addNode("a", { ts: 200 }); // later sighting
		expect(graph.getNode("a").firstTs).toBe(50);
		expect(graph.getNode("a").lastTs).toBe(200);
		graph.addNode("a", { ts: 150 }); // inside the range: no change
		expect(graph.getNode("a").firstTs).toBe(50);
		expect(graph.getNode("a").lastTs).toBe(200);
	});

	test("metadata upgrade: a sighting WITHOUT ts leaves the range alone", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { ts: 100 });
		graph.addNode("a", { rank: 0 }); // no ts on this sighting
		expect(graph.getNode("a").firstTs).toBe(100);
		expect(graph.getNode("a").lastTs).toBe(100);
	});

	test("metadata upgrade: isUser can only turn ON, never off", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { isUser: false });
		graph.addNode("a", { isUser: true });
		expect(graph.getNode("a").isUser).toBe(true);
		graph.addNode("a", { isUser: false }); // a later anon-looking sighting
		expect(graph.getNode("a").isUser).toBe(true);
	});

	test("ts of 0 is a real timestamp, not 'missing'", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { ts: 100 });
		graph.addNode("a", { ts: 0 });
		expect(graph.getNode("a").firstTs).toBe(0);
		expect(graph.getNode("a").lastTs).toBe(100);
	});

	test("rank of 0 is honored on first insert (not replaced by the default)", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { rank: 0 });
		expect(graph.getNode("a").rank).toBe(0);
	});

	test("non-string and empty-string ids are rejected", () => {
		const graph = new IdentityGraph();
		expect(graph.addNode(42)).toBe(false);
		expect(graph.addNode(null)).toBe(false);
		expect(graph.addNode(undefined)).toBe(false);
		expect(graph.addNode({ id: "a" })).toBe(false);
		expect(graph.addNode("")).toBe(false);
		expect(graph.size).toBe(0);
	});

	test("getNode returns null for unknown ids and a COPY for known ones", () => {
		const graph = new IdentityGraph();
		expect(graph.getNode("nope")).toBe(null);
		graph.addNode("a", { rank: 1, ts: 5 });
		const meta = graph.getNode("a");
		meta.rank = 0; // mutating the copy must not touch the graph
		expect(graph.getNode("a").rank).toBe(1);
	});
});

describe("addEdge + transitivity", () => {
	test("edge between two known nodes returns true and unions them", () => {
		const graph = new IdentityGraph();
		graph.addNode("a");
		graph.addNode("b");
		expect(graph.addEdge("a", "b")).toBe(true);
		expect(graph.clusters().length).toBe(1);
	});

	test("edge with a missing side returns false (nodes must be added first)", () => {
		const graph = new IdentityGraph();
		graph.addNode("a");
		expect(graph.addEdge("a", "ghost")).toBe(false);
		expect(graph.addEdge("ghost", "a")).toBe(false);
		expect(graph.addEdge("ghost", "phantom")).toBe(false);
		// below the cap this is a caller bug, NOT overflow
		expect(graph.overflowEdges).toBe(0);
	});

	test("repeated edges are idempotent successes", () => {
		const graph = new IdentityGraph();
		graph.addNode("a");
		graph.addNode("b");
		expect(graph.addEdge("a", "b")).toBe(true);
		expect(graph.addEdge("a", "b")).toBe(true);
		expect(graph.addEdge("b", "a")).toBe(true);
		expect(graph.clusters().length).toBe(1);
	});

	test("self-edge is a no-op success", () => {
		const graph = new IdentityGraph();
		graph.addNode("a");
		expect(graph.addEdge("a", "a")).toBe(true);
		expect(graph.clusters().length).toBe(1);
	});

	test("chain a-b-c-d: transitive closure resolves every member to the one user", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { isUser: false, ts: 1, rank: 1 });
		graph.addNode("b", { isUser: false, ts: 2, rank: 1 });
		graph.addNode("c", { isUser: false, ts: 3, rank: 1 });
		graph.addNode("d", { isUser: true, ts: 4, rank: 1 });
		graph.addEdge("a", "b");
		graph.addEdge("b", "c");
		graph.addEdge("c", "d");
		expect(graph.resolve("a")).toBe("d");
		expect(graph.resolve("b")).toBe("d");
		expect(graph.resolve("c")).toBe("d");
		expect(graph.resolve("d")).toBe("d");
		const clusters = graph.clusters();
		expect(clusters.length).toBe(1);
		expect(clusters[0].members.sort()).toEqual(["a", "b", "c", "d"]);
		expect(clusters[0].users).toEqual([{ id: "d", rank: 1, ts: 4 }]);
	});

	test("chain built in REVERSE edge order still closes transitively", () => {
		const graph = new IdentityGraph();
		["a", "b", "c", "d"].forEach((id) => graph.addNode(id, { isUser: id === "a", ts: 1, rank: 1 }));
		graph.addEdge("c", "d");
		graph.addEdge("b", "c");
		graph.addEdge("a", "b");
		expect(graph.resolve("d")).toBe("a");
	});

	test("fan-in: many devices onto one user form a single cluster", () => {
		const graph = new IdentityGraph();
		graph.addNode("user-1", { isUser: true, ts: 10, rank: 1 });
		const devices = ["dev-1", "dev-2", "dev-3", "dev-4", "dev-5"];
		for (const device of devices) {
			graph.addNode(device, { isUser: false, ts: 1, rank: 1 });
			graph.addEdge(device, "user-1");
		}
		const clusters = graph.clusters();
		expect(clusters.length).toBe(1);
		expect(clusters[0].members.length).toBe(6);
		expect(clusters[0].users).toEqual([{ id: "user-1", rank: 1, ts: 10 }]);
		for (const device of devices) expect(graph.resolve(device)).toBe("user-1");
	});

	test("merging two existing clusters collapses them into one", () => {
		const graph = new IdentityGraph();
		["a1", "a2", "b1", "b2"].forEach((id) => graph.addNode(id, { ts: 1, rank: 1 }));
		graph.addNode("user-b", { isUser: true, ts: 5, rank: 1 });
		graph.addEdge("a1", "a2"); // anon-only cluster
		graph.addEdge("b1", "b2");
		graph.addEdge("b2", "user-b"); // resolved cluster
		expect(graph.clusters().length).toBe(2);
		expect(graph.resolve("a1")).toBe(null); // anon-only: no user yet

		// the bridging edge (e.g. a late $merge) unifies them
		graph.addEdge("a2", "b1");
		const clusters = graph.clusters();
		expect(clusters.length).toBe(1);
		expect(clusters[0].members.length).toBe(5);
		expect(graph.resolve("a1")).toBe("user-b"); // retroactively resolved
	});

	test("independent clusters stay independent", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { isUser: true, ts: 1, rank: 1 });
		graph.addNode("a-dev", { ts: 1, rank: 1 });
		graph.addNode("b", { isUser: true, ts: 1, rank: 1 });
		graph.addNode("b-dev", { ts: 1, rank: 1 });
		graph.addEdge("a", "a-dev");
		graph.addEdge("b", "b-dev");
		expect(graph.clusters().length).toBe(2);
		expect(graph.resolve("a-dev")).toBe("a");
		expect(graph.resolve("b-dev")).toBe("b");
	});
});

describe("clusters()", () => {
	test("includes single-member clusters (caller filters, not the graph)", () => {
		const graph = new IdentityGraph();
		graph.addNode("solo-anon", { ts: 1, rank: 2 });
		graph.addNode("solo-user", { isUser: true, ts: 2, rank: 0 });
		const clusters = graph.clusters();
		expect(clusters.length).toBe(2);
		const members = clusters.map((c) => c.members).flat().sort();
		expect(members).toEqual(["solo-anon", "solo-user"]);
	});

	test("every node appears in exactly one cluster", () => {
		const graph = new IdentityGraph();
		const ids = ["a", "b", "c", "d", "e", "f", "g"];
		ids.forEach((id) => graph.addNode(id, { ts: 1, rank: 1 }));
		graph.addEdge("a", "b");
		graph.addEdge("c", "d");
		graph.addEdge("d", "e");
		// f, g stay singletons
		const clusters = graph.clusters();
		const allMembers = clusters.map((c) => c.members).flat().sort();
		expect(allMembers).toEqual(ids);
		expect(clusters.length).toBe(4); // {a,b} {c,d,e} {f} {g}
	});

	test("users array carries { id, rank, ts } election inputs (ts = lastTs)", () => {
		const graph = new IdentityGraph();
		graph.addNode("u", { isUser: true, ts: 10, rank: 1 });
		graph.addNode("u", { isUser: true, ts: 99, rank: 0 }); // upgrades
		graph.addNode("anon", { ts: 1, rank: 2 });
		graph.addEdge("u", "anon");
		const [cluster] = graph.clusters();
		expect(cluster.users).toEqual([{ id: "u", rank: 0, ts: 99 }]);
	});

	test("multi-user cluster lists EVERY user", () => {
		const graph = new IdentityGraph();
		graph.addNode("u1", { isUser: true, ts: 1, rank: 1 });
		graph.addNode("u2", { isUser: true, ts: 2, rank: 0 });
		graph.addNode("shared-device", { ts: 1, rank: 1 });
		graph.addEdge("u1", "shared-device");
		graph.addEdge("u2", "shared-device");
		const [cluster] = graph.clusters();
		expect(cluster.users.length).toBe(2);
		expect(cluster.users.map((u) => u.id).sort()).toEqual(["u1", "u2"]);
	});

	test("empty graph yields an empty array", () => {
		expect(new IdentityGraph().clusters()).toEqual([]);
	});
});

describe("electUser", () => {
	test("empty / missing users elect no one", () => {
		expect(IdentityGraph.electUser([])).toBe(null);
		expect(IdentityGraph.electUser(undefined)).toBe(null);
		expect(IdentityGraph.electUser(null)).toBe(null);
	});

	test("single user is elected regardless of mode", () => {
		const users = [{ id: "u", rank: 2, ts: null }];
		expect(IdentityGraph.electUser(users)).toBe("u");
		expect(IdentityGraph.electUser(users, "resolve")).toBe("u");
		expect(IdentityGraph.electUser(users, "drop")).toBe("u");
	});

	test("RANK beats recency: hard dual-ID row (0) wins over a fresher verb edge (1)", () => {
		const users = [
			{ id: "verb-user-recent", rank: 1, ts: 9999 },
			{ id: "hard-user-old", rank: 0, ts: 1 }
		];
		expect(IdentityGraph.electUser(users)).toBe("hard-user-old");
	});

	test("recency beats lexicographic: same rank, later lastTs wins", () => {
		const users = [
			{ id: "aaa", rank: 1, ts: 100 },
			{ id: "zzz", rank: 1, ts: 200 }
		];
		expect(IdentityGraph.electUser(users)).toBe("zzz");
	});

	test("lexicographic min is the final tiebreak: same rank, same ts", () => {
		const users = [
			{ id: "charlie", rank: 1, ts: 100 },
			{ id: "alice", rank: 1, ts: 100 },
			{ id: "bob", rank: 1, ts: 100 }
		];
		expect(IdentityGraph.electUser(users)).toBe("alice");
	});

	test("full ordering: rank asc → lastTs desc → id asc", () => {
		const users = [
			{ id: "d-rank2-newest", rank: 2, ts: 99999 },
			{ id: "c-rank1-old", rank: 1, ts: 10 },
			{ id: "b-rank0-old", rank: 0, ts: 10 },
			{ id: "a-rank0-new", rank: 0, ts: 20 }
		];
		expect(IdentityGraph.electUser(users)).toBe("a-rank0-new");
	});

	test("null ts loses to any real ts at the same rank", () => {
		const users = [
			{ id: "no-ts", rank: 1, ts: null },
			{ id: "has-ts", rank: 1, ts: 1 }
		];
		expect(IdentityGraph.electUser(users)).toBe("has-ts");
	});

	test("two null-ts users at the same rank fall through to lexicographic", () => {
		const users = [
			{ id: "beta", rank: 1, ts: null },
			{ id: "alpha", rank: 1, ts: null }
		];
		expect(IdentityGraph.electUser(users)).toBe("alpha");
	});

	test("mode 'drop': multi-user clusters elect NO ONE", () => {
		const users = [
			{ id: "u1", rank: 0, ts: 1 },
			{ id: "u2", rank: 1, ts: 2 }
		];
		expect(IdentityGraph.electUser(users, "drop")).toBe(null);
	});

	test("mode 'error' is the caller's job: election still runs (caller checks users.length)", () => {
		const users = [
			{ id: "u1", rank: 0, ts: 1 },
			{ id: "u2", rank: 1, ts: 2 }
		];
		expect(IdentityGraph.electUser(users, "error")).toBe("u1");
	});

	test("does not mutate the input array", () => {
		const users = [
			{ id: "z", rank: 1, ts: 1 },
			{ id: "a", rank: 0, ts: 1 }
		];
		const snapshot = users.map((u) => u.id);
		IdentityGraph.electUser(users);
		expect(users.map((u) => u.id)).toEqual(snapshot);
	});

	test("instance method delegates to the static", () => {
		const graph = new IdentityGraph();
		const users = [
			{ id: "u1", rank: 0, ts: 1 },
			{ id: "u2", rank: 1, ts: 2 }
		];
		expect(graph.electUser(users)).toBe(IdentityGraph.electUser(users));
		expect(graph.electUser(users, "drop")).toBe(null);
	});
});

describe("resolve", () => {
	test("unknown id resolves to null", () => {
		const graph = new IdentityGraph();
		expect(graph.resolve("ghost")).toBe(null);
		graph.addNode("a");
		expect(graph.resolve("ghost")).toBe(null);
	});

	test("non-string id resolves to null (no throw)", () => {
		const graph = new IdentityGraph();
		expect(graph.resolve(42)).toBe(null);
		expect(graph.resolve(null)).toBe(null);
		expect(graph.resolve(undefined)).toBe(null);
	});

	test("anon-only cluster resolves to null", () => {
		const graph = new IdentityGraph();
		graph.addNode("a", { ts: 1, rank: 2 });
		graph.addNode("b", { ts: 2, rank: 2 });
		graph.addEdge("a", "b");
		expect(graph.resolve("a")).toBe(null);
		expect(graph.resolve("b")).toBe(null);
	});

	test("a user resolves to itself", () => {
		const graph = new IdentityGraph();
		graph.addNode("u", { isUser: true, ts: 1, rank: 0 });
		expect(graph.resolve("u")).toBe("u");
	});

	test("multi-user cluster: default mode elects, mode 'drop' returns null", () => {
		const graph = new IdentityGraph();
		graph.addNode("u1", { isUser: true, ts: 1, rank: 0 });
		graph.addNode("u2", { isUser: true, ts: 999, rank: 1 });
		graph.addNode("device", { ts: 1, rank: 1 });
		graph.addEdge("device", "u1");
		graph.addEdge("device", "u2");
		expect(graph.resolve("device")).toBe("u1"); // rank 0 beats fresher rank 1
		expect(graph.resolve("device", "drop")).toBe(null);
	});

	test("resolution is live: metadata upgrades can change the winner", () => {
		const graph = new IdentityGraph();
		graph.addNode("u1", { isUser: true, ts: 1, rank: 1 });
		graph.addNode("u2", { isUser: true, ts: 2, rank: 1 });
		graph.addNode("device", { ts: 1, rank: 1 });
		graph.addEdge("device", "u1");
		graph.addEdge("device", "u2");
		expect(graph.resolve("device")).toBe("u2"); // same rank, u2 is fresher
		graph.addNode("u1", { isUser: true, ts: 1, rank: 0 }); // hard evidence arrives for u1
		expect(graph.resolve("device")).toBe("u1"); // rank now wins
	});
});

describe("maxNodes cap + overflow", () => {
	test("new nodes are rejected at cap; size stops growing", () => {
		const graph = new IdentityGraph({ maxNodes: 3 });
		expect(graph.addNode("a")).toBe(true);
		expect(graph.addNode("b")).toBe(true);
		expect(graph.addNode("c")).toBe(true);
		expect(graph.addNode("d")).toBe(false);
		expect(graph.addNode("e")).toBe(false);
		expect(graph.size).toBe(3);
	});

	test("EXISTING nodes are always accepted at cap (idempotent re-add)", () => {
		const graph = new IdentityGraph({ maxNodes: 2 });
		graph.addNode("a", { rank: 2, ts: 10 });
		graph.addNode("b");
		expect(graph.addNode("a", { rank: 0, ts: 99, isUser: true })).toBe(true);
		expect(graph.size).toBe(2);
		// and the metadata upgrade still applies at cap
		expect(graph.getNode("a")).toEqual({ isUser: true, rank: 0, firstTs: 10, lastTs: 99 });
	});

	test("addEdge involving a cap-rejected node fails and counts in overflowEdges", () => {
		const graph = new IdentityGraph({ maxNodes: 2 });
		graph.addNode("a");
		graph.addNode("b");
		expect(graph.addNode("rejected")).toBe(false);
		expect(graph.addEdge("a", "rejected")).toBe(false);
		expect(graph.overflowEdges).toBe(1);
	});

	test("EVERY failed edge at cap is counted, including repeats", () => {
		const graph = new IdentityGraph({ maxNodes: 1 });
		graph.addNode("a");
		graph.addNode("x"); // rejected
		graph.addNode("y"); // rejected
		graph.addEdge("a", "x");
		graph.addEdge("a", "x"); // same failing edge again: counted again
		graph.addEdge("a", "y");
		graph.addEdge("x", "y"); // both sides rejected: still one skipped edge
		expect(graph.overflowEdges).toBe(4);
	});

	test("edges between existing nodes still work at cap and do NOT count", () => {
		const graph = new IdentityGraph({ maxNodes: 2 });
		graph.addNode("a", { isUser: true, ts: 1, rank: 1 });
		graph.addNode("b", { ts: 1, rank: 1 });
		graph.addNode("rejected");
		expect(graph.addEdge("a", "b")).toBe(true);
		expect(graph.overflowEdges).toBe(0);
		expect(graph.resolve("b")).toBe("a");
	});

	test("overflowEdges starts at 0 and stays 0 below the cap", () => {
		const graph = new IdentityGraph({ maxNodes: 100 });
		graph.addNode("a");
		expect(graph.overflowEdges).toBe(0);
		graph.addEdge("a", "never-added"); // caller bug, not overflow
		expect(graph.overflowEdges).toBe(0);
	});

	test("atCapacity flips exactly when size hits maxNodes", () => {
		const graph = new IdentityGraph({ maxNodes: 2 });
		expect(graph.atCapacity).toBe(false);
		graph.addNode("a");
		expect(graph.atCapacity).toBe(false);
		graph.addNode("b");
		expect(graph.atCapacity).toBe(true);
	});

	test("default cap is 5,000,000", () => {
		expect(new IdentityGraph().maxNodes).toBe(5_000_000);
	});
});

describe("denylist is NOT this module's job", () => {
	// the design contract puts denylist filtering in the identity-replay stage
	// (the caller), which excludes + counts denylisted ids BEFORE they reach the
	// graph. the graph itself must accept any string id without prejudice.
	test("the graph accepts test-account-looking ids like any other id", () => {
		const graph = new IdentityGraph();
		expect(graph.addNode("test-account@company.com", { isUser: true, ts: 1, rank: 1 })).toBe(true);
		expect(graph.addNode("anonymous", { ts: 1, rank: 2 })).toBe(true);
		expect(graph.addNode("00000000-0000-0000-0000-000000000000", { ts: 1, rank: 2 })).toBe(true);
		graph.addEdge("anonymous", "test-account@company.com");
		expect(graph.resolve("anonymous")).toBe("test-account@company.com");
	});

	test("the constructor exposes no denylist option", () => {
		const graph = new IdentityGraph({ maxNodes: 10, denylist: ["should-be-ignored"] });
		expect(graph.denylist).toBeUndefined();
		expect(graph.addNode("should-be-ignored")).toBe(true); // no filtering here
	});
});

describe("module surface", () => {
	test("exports exactly IdentityGraph and stripDevicePrefix", () => {
		const exported = require("../components/identity-graph.js");
		expect(Object.keys(exported).sort()).toEqual(["IdentityGraph", "stripDevicePrefix"]);
		expect(typeof exported.IdentityGraph).toBe("function");
		expect(typeof exported.stripDevicePrefix).toBe("function");
	});

	test("has no internal require()s and no process-global handlers (static scan)", () => {
		const fs = require("fs");
		const path = require("path");
		const src = fs.readFileSync(path.join(__dirname, "..", "components", "identity-graph.js"), "utf8");
		expect(src).not.toMatch(/require\(/);
		expect(src).not.toMatch(/process\.(on|once|prependListener)\(/);
	});
});

describe("perf smoke", () => {
	// disabled by default: it allocates ~1M Map entries (~a few hundred MB) and
	// takes a few seconds. to run it, change `test.skip` to `test` (or `test.only`)
	// and run:  npx jest tests/identity-graph.test.js -t "1M-node"
	// (remember: jest is normally run BY THE USER, not by the agent)
	test.skip("1M-node chain: build + full resolution stays fast and correct", () => {
		const NODES = 1_000_000;
		const graph = new IdentityGraph({ maxNodes: NODES + 1 });
		const started = Date.now();

		// one user at the head, then a long anon chain hanging off it
		graph.addNode("user-0", { isUser: true, ts: 0, rank: 0 });
		let previous = "user-0";
		for (let i = 1; i < NODES; i++) {
			const id = `anon-${i}`;
			graph.addNode(id, { isUser: false, ts: i, rank: 1 });
			graph.addEdge(previous, id);
			previous = id;
		}

		expect(graph.size).toBe(NODES);
		// resolving the far end forces the longest find path (then compresses it)
		expect(graph.resolve(`anon-${NODES - 1}`)).toBe("user-0");
		expect(graph.resolve("anon-1")).toBe("user-0");

		const clusters = graph.clusters();
		expect(clusters.length).toBe(1);
		expect(clusters[0].members.length).toBe(NODES);
		expect(clusters[0].users).toEqual([{ id: "user-0", rank: 0, ts: 0 }]);

		const elapsed = Date.now() - started;
		// generous bound: union by size + path compression keeps this in low seconds
		expect(elapsed).toBeLessThan(30_000);
	}, 60_000);
});
