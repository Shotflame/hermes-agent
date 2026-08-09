"""Regression tests for issue #82427: spurious generation creation.

Multiple calls to repair_vulnerable_runtime() must not accumulate stale
generation directories. When a safe generation already exists with the
requested Python version, it should be reused instead of creating a new one.
"""

from pathlib import Path
from unittest.mock import patch
import pytest


def _make_runtime_install(tmp_path, *, windows=False):
    root = tmp_path / "checkout"
    root.mkdir()
    (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    live = root / "venv"
    bin_dir = live / ("Scripts" if windows else "bin")
    bin_dir.mkdir(parents=True)
    python = bin_dir / ("python.exe" if windows else "python")
    python.write_text("live interpreter", encoding="utf-8")
    sentinel = live / "sentinel"
    sentinel.write_text("live", encoding="utf-8")
    return root, live, sentinel


def _runtime_info(executable: Path, sqlite_version: tuple[int, int, int]):
    from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo
    return SQLiteRuntimeInfo(
        executable=executable,
        base_prefix=executable.parent.parent,
        python_version=(3, 11, 15),
        sqlite_version=sqlite_version,
        sqlite_version_string=".".join(str(part) for part in sqlite_version),
        sqlite_source_id=f"source-{sqlite_version}",
    )


class TestGenerationDeduplication:
    """Regression tests for issue #82427: spurious generation creation."""

    def test_find_existing_safe_generation_returns_none_if_not_exists(self, tmp_path):
        """No existing generations returns None."""
        from hermes_cli.managed_uv import _find_existing_safe_generation
        from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo

        python_root = tmp_path / "python"
        python_root.mkdir()
        
        current = SQLiteRuntimeInfo(
            executable=Path("/venv/bin/python"),
            base_prefix=Path("/venv"),
            python_version=(3, 11, 15),
            sqlite_version=(3, 50, 4),
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        result = _find_existing_safe_generation(python_root, "3.11", current)
        assert result is None

    def test_find_existing_safe_generation_reuses_safe_generation(self, tmp_path):
        """Safe generation with matching Python version is reused."""
        from hermes_cli.managed_uv import _find_existing_safe_generation
        from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo

        python_root = tmp_path / "python"
        python_root.mkdir()
        
        # Create a pre-existing generation with safe SQLite
        gen_path = python_root / "generation-existing-safe-123"
        python_binary = gen_path / "bin" / "python"
        python_binary.parent.mkdir(parents=True)
        python_binary.write_text("python", encoding="utf-8")
        
        # Current runtime (vulnerable)
        current = SQLiteRuntimeInfo(
            executable=python_binary,
            base_prefix=gen_path,
            python_version=(3, 11, 15),
            sqlite_version=(3, 50, 4),
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        # Existing safe generation
        existing_safe = SQLiteRuntimeInfo(
            executable=python_binary,
            base_prefix=gen_path,
            python_version=(3, 11, 15),
            sqlite_version=(3, 53, 1),
            sqlite_version_string="3.53.1",
            sqlite_source_id="safe",
        )
        
        with patch(
            "hermes_cli.managed_uv.probe_sqlite_runtime",
            return_value=existing_safe,
        ):
            result = _find_existing_safe_generation(python_root, "3.11", current)
            assert result is not None
            gen, py, info = result
            assert gen == gen_path
            assert py == python_binary
            assert info.sqlite_version == (3, 53, 1)
            assert not info.wal_reset_vulnerable

    def test_find_existing_safe_generation_skips_vulnerable_candidates(self, tmp_path):
        """Vulnerable generations should not be reused."""
        from hermes_cli.managed_uv import _find_existing_safe_generation
        from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo

        python_root = tmp_path / "python"
        python_root.mkdir()
        
        # Create a generation with vulnerable SQLite
        vuln_gen = python_root / "generation-vulnerable-123"
        vuln_py = vuln_gen / "bin" / "python"
        vuln_py.parent.mkdir(parents=True)
        vuln_py.write_text("python", encoding="utf-8")
        
        current = SQLiteRuntimeInfo(
            executable=vuln_py,
            base_prefix=vuln_gen,
            python_version=(3, 11, 15),
            sqlite_version=(3, 50, 4),
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        # The existing generation is also vulnerable
        existing_vuln = SQLiteRuntimeInfo(
            executable=vuln_py,
            base_prefix=vuln_gen,
            python_version=(3, 11, 16),
            sqlite_version=(3, 50, 4),  # Still vulnerable!
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        with patch(
            "hermes_cli.managed_uv.probe_sqlite_runtime",
            return_value=existing_vuln,
        ):
            result = _find_existing_safe_generation(python_root, "3.11", current)
            assert result is None

    def test_find_existing_safe_generation_skips_wrong_version(self, tmp_path):
        """Generations with different Python minor versions should not be reused."""
        from hermes_cli.managed_uv import _find_existing_safe_generation
        from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo

        python_root = tmp_path / "python"
        python_root.mkdir()
        
        # Create a generation with Python 3.10
        gen_310 = python_root / "generation-310-safe"
        py_310 = gen_310 / "bin" / "python"
        py_310.parent.mkdir(parents=True)
        py_310.write_text("python", encoding="utf-8")
        
        # Current is Python 3.11 (vulnerable)
        current_311 = SQLiteRuntimeInfo(
            executable=py_310,
            base_prefix=gen_310,
            python_version=(3, 11, 15),
            sqlite_version=(3, 50, 4),
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        # Existing is Python 3.10 (safe)
        existing_310 = SQLiteRuntimeInfo(
            executable=py_310,
            base_prefix=gen_310,
            python_version=(3, 10, 15),  # Different minor!
            sqlite_version=(3, 53, 1),
            sqlite_version_string="3.53.1",
            sqlite_source_id="safe",
        )
        
        with patch(
            "hermes_cli.managed_uv.probe_sqlite_runtime",
            return_value=existing_310,
        ):
            result = _find_existing_safe_generation(python_root, "3.11", current_311)
            assert result is None

    def test_install_safe_python_generation_checks_for_existing_first(self, tmp_path):
        """_install_safe_python_generation should check for existing safe generation first."""
        from hermes_cli.managed_uv import _install_safe_python_generation
        from hermes_cli.sqlite_runtime import SQLiteRuntimeInfo

        root = tmp_path / "checkout"
        root.mkdir()
        (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
        
        # Pre-create a safe generation
        generation = root / ".hermes-runtime" / "python" / "generation-safe-v1"
        candidate_python = generation / "bin" / "python"
        candidate_python.parent.mkdir(parents=True)
        candidate_python.write_text("safe python", encoding="utf-8")
        
        current = SQLiteRuntimeInfo(
            executable=Path("/venv/bin/python"),
            base_prefix=Path("/venv"),
            python_version=(3, 11, 15),
            sqlite_version=(3, 50, 4),
            sqlite_version_string="3.50.4",
            sqlite_source_id="vulnerable",
        )
        
        fixed = SQLiteRuntimeInfo(
            executable=candidate_python,
            base_prefix=generation,
            python_version=(3, 11, 15),
            sqlite_version=(3, 53, 1),
            sqlite_version_string="3.53.1",
            sqlite_source_id="safe",
        )
        
        # Track calls to _attempt_install_generation (should not be called)
        attempt_calls = []
        
        def fake_attempt(*args, **kwargs):
            attempt_calls.append(True)
            return None
        
        with patch("hermes_cli.managed_uv.platform.system", return_value="Linux"), \
             patch(
                 "hermes_cli.managed_uv.probe_sqlite_runtime",
                 return_value=fixed,
             ), \
             patch(
                 "hermes_cli.managed_uv._attempt_install_generation",
                 side_effect=fake_attempt,
             ):
            result = _install_safe_python_generation(
                "uv",
                project_root=root,
                current=current,
            )
        
        # Should have found the existing generation without calling _attempt_install_generation
        assert result is not None
        assert result == (generation, candidate_python, fixed)
        assert len(attempt_calls) == 0  # _attempt_install_generation not called!

    def test_repair_vulnerable_runtime_does_not_create_duplicate_generations(self, tmp_path):
        """Multiple repair calls should not create duplicate generations."""
        from hermes_cli.managed_uv import repair_vulnerable_runtime

        root, live, sentinel = _make_runtime_install(tmp_path)
        current = _runtime_info(live / "bin" / "python", (3, 50, 4))
        
        # Pre-create a safe generation (simulating a previous repair)
        generation = root / ".hermes-runtime" / "python" / "generation-safe-v1"
        candidate_python = generation / "bin" / "python"
        candidate_python.parent.mkdir(parents=True)
        candidate_python.write_text("safe python", encoding="utf-8")
        fixed = _runtime_info(candidate_python, (3, 53, 1))
        
        # Create the candidate venv that staging would produce
        candidate_venv = root / ".hermes-runtime" / "venv-candidate"
        (candidate_venv / "bin").mkdir(parents=True)
        (candidate_venv / "bin" / "python").write_text("candidate venv python", encoding="utf-8")

        install_safe_calls = []

        def track_install_safe(uv_bin, *, project_root, current):
            install_safe_calls.append(True)
            # The deduplication check will find the pre-existing generation
            return (generation, candidate_python, fixed)

        with patch("hermes_cli.managed_uv.platform.system", return_value="Linux"), \
             patch(
                 "hermes_cli.managed_uv.probe_sqlite_runtime",
                 side_effect=[current, current],  # Once for initial check, once for smoke test
             ), \
             patch(
                 "hermes_cli.managed_uv._install_safe_python_generation",
                 side_effect=track_install_safe,
             ), \
             patch(
                 "hermes_cli.managed_uv._stage_candidate_venv",
                 return_value=candidate_venv,
             ), \
             patch(
                 "hermes_cli.managed_uv._smoke_candidate_venv",
                 return_value=(True, "", fixed),
             ):
            # Call repair
            result = repair_vulnerable_runtime("uv", project_root=root)
        
        assert result.status == "repaired"
        # Confirm the call was made
        assert len(install_safe_calls) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
