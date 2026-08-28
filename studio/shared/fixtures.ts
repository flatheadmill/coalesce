import type { DagResponse, Run, RunDetail } from "./api";

export const runFixtures: Run[] = [
  {
    slug: "widget-press-104",
    pipeline: "widget-press.coalesce.zsh",
    started_at: "2026-08-28T13:40:00Z",
    status: "running",
  },
  {
    slug: "cracked-widget-103",
    pipeline: "cracked-widget.coalesce.zsh",
    started_at: "2026-08-28T13:20:00Z",
    completed_at: "2026-08-28T13:20:05Z",
    status: "failed",
  },
  {
    slug: "hello-102",
    pipeline: "hello.coalesce.zsh",
    started_at: "2026-08-28T13:00:00Z",
    completed_at: "2026-08-28T13:00:06Z",
    status: "completed",
  },
];

export const runDetailFixture: RunDetail = {
  ...runFixtures[0],
  jobs: [
    {
      job: "coalesce.prepare",
      started_at: "2026-08-28T13:40:00Z",
      completed_at: "2026-08-28T13:40:04Z",
      status: "completed",
      exit_code: 0,
    },
    {
      job: "coalesce.press",
      started_at: "2026-08-28T13:40:04Z",
      status: "running",
    },
  ],
};

export const dagFixture: DagResponse = {
  created_at: "2026-08-28T13:40:00Z",
  dag: [
    {
      name: "prepare",
      under: "coalesce",
      kind: "node",
      children: [],
    },
    {
      name: "presses",
      under: "coalesce",
      kind: "tranche",
      parallel: true,
      children: [
        {
          name: "press",
          under: "coalesce.presses",
          kind: "node",
          children: [],
        },
      ],
    },
  ],
};

export const logFixture = `preparing widget press
pressing widget 1
pressing widget 2
`;
