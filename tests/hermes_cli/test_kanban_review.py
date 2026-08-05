"""Review-lane tests: request_review transition + CLI verb + worker tool.

The review lane was dead code: nothing could move a task into ``review``
(the dispatcher's review-column dispatch + ``claim_review_task`` solver
existed, but no entry point). These tests pin the newly-wired path:
running/ready -> review with optional reviewer reassignment, event
emission, run closure, and the guarded transitions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def test_request_review_from_ready_reassigns_and_emits_event(kanban_home):
    with kb.connect() as conn:
        t = kb.create_task(conn, title="ship feature", assignee="builder")
        assert kb.get_task(conn, t).status == "ready"

        landed = kb.request_review(
            conn, t, reason="please verify diff", reviewer="reviewer"
        )
        assert landed == "review"

        task = kb.get_task(conn, t)
        assert task is not None and task.status == "review"
        assert task is not None and task.assignee == "reviewer"  # reassigned

        events = kb.list_events(conn, t)
        assert any(
            e.kind == "review_requested"
            and e.payload == {"reason": "please verify diff", "reviewer": "reviewer"}
            for e in events
        )

        # Dispatch solver can now claim it -> the lane is alive.
        claimed = kb.claim_review_task(conn, t)
        assert claimed is not None
        assert kb.get_task(conn, t).status == "running"


def test_request_review_keeps_assignee_when_reviewer_omitted(kanban_home):
    with kb.connect() as conn:
        t = kb.create_task(conn, title="doc polish", assignee="writer")
        landed = kb.request_review(conn, t, reason="check formatting")
        assert landed == "review"
        task = kb.get_task(conn, t)
        assert task is not None and task.assignee == "writer"


def test_request_review_rejects_terminal_states(kanban_home):
    with kb.connect() as conn:
        # done is terminal -> no transition
        t = kb.create_task(conn, title="already done")
        kb.complete_task(conn, t, result="x")
        assert kb.request_review(conn, t, reason="nope") is None
        task = kb.get_task(conn, t)
        assert task is not None and task.status == "done"


def test_request_review_from_running_closes_run(kanban_home):
    with kb.connect() as conn:
        t = kb.create_task(conn, title="long build", assignee="builder")
        claimed = kb.claim_task(conn, t, claimer=f"{kb._claimer_id()}:w")
        assert claimed is not None
        run = kb.latest_run(conn, t)
        assert run is not None
        run_id = run.id

        landed = kb.request_review(conn, t, reason="tests green, verify")
        assert landed == "review"

        run = kb.latest_run(conn, t)
        assert run is not None and run.id == run_id
        assert run is not None and run.outcome in ("review", "blocked")  # ended
        task = kb.get_task(conn, t)
        assert task is not None and task.claim_lock is None


def test_request_review_expected_run_id_guards_stale_claim(kanban_home):
    with kb.connect() as conn:
        t = kb.create_task(conn, title="guarded")
        # ready task, bogus expected_run_id -> no match, no transition
        assert kb.request_review(conn, t, reason="x", expected_run_id=999) is None
        task = kb.get_task(conn, t)
        assert task is not None and task.status == "ready"
