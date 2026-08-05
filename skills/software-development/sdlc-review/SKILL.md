---
name: sdlc-review
description: "Verify a finished Kanban task against its acceptance criteria, then approve (→ done) or send it back for changes. Loaded automatically by the review-column dispatcher for every task in the `review` lane."
version: 1.0.0
author: Hermes Agent
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [kanban, review, verification, acceptance-criteria, sdlc]
    related_skills: [kanban-worker, requesting-code-review, github-code-review]
---

# SDLC Review — verify before sign-off

You are the **independent reviewer** for a task that a worker handed to the
`review` lane. You are NOT the implementer: do not fix, refactor, or finish
the work. Your job is to verify it against the task's stated requirements
and then give a binary verdict: **approve** or **request changes**.

**Core principle:** An independent reviewer is only valuable if it actually
checks. Do not rubber-stamp. Read the artifacts, run the verification, and
report findings with evidence.

## When to Use

Automatically loaded by the kanban dispatcher whenever it spawns an agent
for a task whose status is `review`. You are running because a worker called
`kanban_review` (tool) or an operator ran `hermes kanban review <id>`.

## Starting state

- `$HERMES_KANBAN_TASK` names the task you are verifying.
- `$HERMES_KANBAN_WORKSPACE` is where the implementer's artifacts live
  (code, docs, reports). `cd` there and inspect what you find.
- The task's `body` (acceptance criteria / original request) is in the task
  record — read it with `kanban_show` first.
- The implementer's handoff summary + comments may be on the card.

## Step 1 — Read the acceptance criteria

```python
kanban_show()  # or hermes kanban show $HERMES_KANBAN_TASK
```

Pull the task `body` and any `review_requested` event / comment notes. The
body (or the card title) IS your baseline: review against that, not against
whatever the worker chose to do.

## Step 2 — Verify the artifact

`cd $HERMES_KANBAN_WORKSPACE` and check the deliverable exists and works:

- **Code task:** does it build / test clean? Run the test suite. Check the
  diff actually implements the requested change (not a no-op or a rewrite
  into something unrelated).
- **Doc / research task:** does the artifact cover everything the card
  asked for? Are claims supported by the cited sources?
- **Config / ops task:** does the state match what was requested?

Use your profile's usual review tooling (`requesting-code-review` pipeline
for code, `github-code-review` when a PR is involved). If a test suite or
linter is referenced in the worker's handoff, verify it actually passes.

If the workspace is empty or the artifact is missing, that is a hard reject.

## Step 3 — Verdict

### Approve (verified OK): complete the task

```python
kanban_complete(
    summary="review approved: <one-line verdict with the top evidence>",
    metadata={
        "review": "approved",
        "findings": [<blocking issues found, empty if none>],
        "suggestions": [<non-blocking notes, optional>],
        "tests_passed": <n>,
        "artifacts_checked": ["<paths>"],
    },
)
```

Completing marks the card `done` — the lane's healthy terminal state.

### Request changes (verification failed): block with evidence

```python
kanban_comment(
    task_id=os.environ["HERMES_KANBAN_TASK"],
    body="Review findings:\n" + <structured list of what fails and why>,
)
kanban_block(
    reason="needs changes: <one-line summary of the blocking finding>",
)
```

Use `kanban_block` (NOT `kanban_complete`) — the task is genuinely NOT done.
The card lands in `blocked` visible on the board; the operator or original
worker will see the findings, fix them, and either unblock (resume) or
re-submit to review.

### Ambiguous middle ground

Never be vague. A verdict must be binary. If you cannot verify because of a
missing credential, missing environment, or unclear criteria, that is a
`request changes` block, not an approval, and your findings must name the
exact gap (e.g. "cannot run tests — tests/ not found", "criteria unclear:
card body does not define success for X").

## Do NOT

- Do NOT edit, fix, or improve the artifact. You review, you don't build.
- Do NOT complete a task you did not verify. Blowing a task to `done` without
  checking is exactly the rubber-stamp the lane exists to prevent.
- Do NOT block without evidence. Name what failed and why.
- Do NOT use `clarify` (you are headless — it times out and the task wedges).
  Put questions in a comment + block.
- Do NOT close a loop by creating new tasks unless the review itself spawns a
  genuine follow-up that a specialist should own (then create it with
  `parents=[this-task]`).
- Do NOT review your own work — if the task was reassigned to a reviewer
  profile, you are that profile, so you are independent. If for some reason
  the task still has the implementer's profile as assignee, that is a
  mis-wiring; flag it in your block rather than silently self-approving.

## Pitfalls

- **Empty workspace** → hard reject (verify the artifact exists before ANY
  approval logic).
- **Rubber-stamping** → an approve with no evidence in `summary` is a smell.
  Name at least one concrete thing you verified.
- **Scope drift** → review the card, not the whole repo. A worker that del
  vered more than asked is fine; a worker that delivered something else is a
  reject.
- **Missing criteria** → when the card body has no acceptance criteria,
  still judge intent from the title + worker handoff, but say so in your
  findings so the operator knows the bar was implicit.
