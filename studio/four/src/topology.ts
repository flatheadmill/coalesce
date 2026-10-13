import type { DagNode } from "../../shared/api";

export interface PrecedenceJob {
  id: string;
  label: string;
  order: number;
  rank: number;
  parent?: string;
}

export interface PrecedenceGroup {
  id: string;
  label: string;
  mode: "ordered" | "parallel";
  order: number;
  parent?: string;
}

export interface PrecedenceRelation {
  id: string;
  source: string;
  target: string;
}

export interface PrecedenceTopology {
  jobs: PrecedenceJob[];
  groups: PrecedenceGroup[];
  relations: PrecedenceRelation[];
  roots: string[];
}

const identityOf = (node: DagNode) => `${node.under}.${node.name}`;

export function buildPrecedenceTopology(declaration: DagNode[]): PrecedenceTopology {
  const jobs: PrecedenceJob[] = [];
  const jobPaths = new Map<string, string[]>();
  const groups: PrecedenceGroup[] = [];
  const relations: PrecedenceRelation[] = [];
  const relationIds = new Set<string>();
  let order = 0;

  const connect = (sources: string[], target: string) => {
    for (const source of sources) {
      if (source === target) continue;
      const id = `${source}->${target}`;
      if (relationIds.has(id)) continue;
      relationIds.add(id);
      relations.push({ id, source, target });
    }
  };

  const walkParallel = (nodes: DagNode[], incoming: string[], parent?: string, path: string[] = []): string[] => {
    const exits: string[] = [];
    for (const node of nodes) exits.push(...walkNode(node, incoming, parent, path));
    return exits.length ? exits : incoming;
  };

  const walkSequence = (nodes: DagNode[], incoming: string[], parent?: string, path: string[] = []): string[] => {
    let exits = incoming;
    for (const node of nodes) exits = walkNode(node, exits, parent, path);
    return exits;
  };

  const walkNode = (node: DagNode, incoming: string[], parent: string | undefined, path: string[]): string[] => {
    const identity = identityOf(node);
    const nodeOrder = order++;
    const nodePath = [...path, node.name];

    if (node.kind === "tranche") {
      const groupId = `group:${identity}`;
      groups.push({
        id: groupId,
        label: node.name,
        mode: node.parallel ? "parallel" : "ordered",
        order: nodeOrder,
        parent,
      });
      return node.parallel
        ? walkParallel(node.children ?? [], incoming, groupId, nodePath)
        : walkSequence(node.children ?? [], incoming, groupId, nodePath);
    }

    jobs.push({ id: identity, label: node.name, order: nodeOrder, rank: 0, parent });
    jobPaths.set(identity, nodePath);
    connect(incoming, identity);
    if (!node.children?.length) return [identity];
    return node.parallel
      ? walkParallel(node.children, [identity], parent, nodePath)
      : walkSequence(node.children, [identity], parent, nodePath);
  };

  walkSequence(declaration, []);
  for (const job of jobs) {
    const path = jobPaths.get(job.id) ?? [job.label];
    for (let length = 1; length <= path.length; length += 1) {
      const suffix = path.slice(-length).join("/");
      const matches = jobs.filter((candidate) =>
        (jobPaths.get(candidate.id) ?? [candidate.label]).slice(-length).join("/") === suffix,
      );
      if (matches.length === 1) {
        job.label = suffix;
        break;
      }
      if (length === path.length) job.label = job.id;
    }
  }
  const targets = new Set(relations.map((relation) => relation.target));
  const roots = jobs.filter((job) => !targets.has(job.id)).map((job) => job.id);
  const ranks = new Map(jobs.map((job) => [job.id, 0]));
  for (const job of jobs) {
    const predecessors = relations.filter((relation) => relation.target === job.id);
    const rank = predecessors.reduce((maximum, relation) => Math.max(maximum, (ranks.get(relation.source) ?? 0) + 1), 0);
    job.rank = rank;
    ranks.set(job.id, rank);
  }
  return { jobs, groups, relations, roots };
}
