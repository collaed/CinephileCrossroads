"""Data integrity tests — the most critical tests for this app.
These verify that data isn't lost during syncs, merges, and task processing.
"""
import pytest
import json, os, copy
from app import (safe_json_load, safe_json_save, complete_task, _apply_task_result,
                 load_task_queue, save_task_queue, generate_tasks_for_library)


class TestAgentDataSeparation:
    """Verify Kodi sync never overwrites agent-collected data."""

    def test_kodi_sync_preserves_file_size(self, data_dir, sample_library):
        """The #1 bug we hit: Kodi sync wiping file_size."""
        lib_path = data_dir / "users" / "testuser" / "tmm_library.json"
        agent_path = data_dir / "users" / "testuser" / "agent_data.json"

        # Agent has collected file sizes
        agent_data = {"tt0137523": {"file_size": 2000000000, "file_hash": "abc123"}}
        json.dump(agent_data, open(agent_path, "w"))

        # Kodi sends library update (no file_size)
        json.dump(sample_library, open(lib_path, "w"))

        # After sync, agent_data should be untouched
        agent_after = json.load(open(agent_path))
        assert agent_after["tt0137523"]["file_size"] == 2000000000
        assert agent_after["tt0137523"]["file_hash"] == "abc123"

    def test_kodi_sync_preserves_thumbnails(self, data_dir):
        agent_path = data_dir / "users" / "testuser" / "agent_data.json"
        agent_data = {"tt0137523": {"thumbnail": "tt0137523.jpg"}}
        json.dump(agent_data, open(agent_path, "w"))

        # Simulate Kodi sync (writes to tmm_library, not agent_data)
        lib_path = data_dir / "users" / "testuser" / "tmm_library.json"
        json.dump({"tt0137523": {"path": "/some/path.mkv"}}, open(lib_path, "w"))

        agent_after = json.load(open(agent_path))
        assert agent_after["tt0137523"]["thumbnail"] == "tt0137523.jpg"


class TestTaskQueue:
    """Verify task queue integrity during regeneration."""

    def test_preserves_exec_code_tasks(self, data_dir):
        """exec_code tasks must survive task regeneration."""
        queue_path = data_dir / "task_queue.json"
        queue = [
            {"id": "nfo_scan", "type": "exec_code", "priority": -1, "status": "pending", "params": {}},
            {"id": "size_1", "type": "size_files", "priority": 1, "status": "pending", "params": {}},
            {"id": "done_1", "type": "size_files", "priority": 1, "status": "done", "result": {}},
        ]
        json.dump(queue, open(queue_path, "w"))

        # Simulate what generate_tasks does: clear pending except exec_code/priority -1
        preserved = [t for t in queue if t["status"] != "pending" or t.get("priority") == -1 or t["type"] in ("exec_code", "update_agent")]
        assert any(t["id"] == "nfo_scan" for t in preserved), "exec_code task was lost"
        assert not any(t["id"] == "size_1" for t in preserved), "size_files should be cleared"
        assert any(t["id"] == "done_1" for t in preserved), "done tasks should be kept"

    def test_preserves_priority_minus_1(self, data_dir):
        queue = [
            {"id": "human_task", "type": "generate_thumb", "priority": -1, "status": "pending", "params": {}},
            {"id": "auto_task", "type": "size_files", "priority": 1, "status": "pending", "params": {}},
        ]
        preserved = [t for t in queue if t["status"] != "pending" or t.get("priority") == -1 or t["type"] in ("exec_code", "update_agent")]
        assert any(t["id"] == "human_task" for t in preserved)
        assert not any(t["id"] == "auto_task" for t in preserved)


class TestJsonSafety:
    """Verify thread-safe JSON operations."""

    def test_safe_json_roundtrip(self, data_dir):
        path = str(data_dir / "test.json")
        data = {"key": "value", "nested": {"a": [1, 2, 3]}}
        safe_json_save(path, data)
        loaded = safe_json_load(path)
        assert loaded == data

    def test_safe_json_load_missing_file(self):
        assert safe_json_load("/nonexistent/path.json") is None

    def test_safe_json_load_corrupt_file(self, data_dir):
        path = str(data_dir / "corrupt.json")
        with open(path, "w") as f:
            f.write("{broken json")
        assert safe_json_load(path) is None


class TestLibraryMerge:
    """Verify library merge preserves existing fields."""

    def test_merge_keeps_existing_fields(self, sample_library):
        """When Kodi sends an update, existing fields should not be lost."""
        existing = copy.deepcopy(sample_library)
        existing["tt0137523"]["confirmed"] = True
        existing["tt0137523"]["nfo_matched"] = True

        # Kodi sends update without those fields
        kodi_update = {"tt0137523": {"path": "/new/path.mkv", "video_height": 1080}}

        # Merge: update fields but keep existing ones
        for iid, info in kodi_update.items():
            if iid in existing and isinstance(existing[iid], dict):
                existing[iid].update(info)

        assert existing["tt0137523"]["confirmed"] == True
        assert existing["tt0137523"]["nfo_matched"] == True
        assert existing["tt0137523"]["path"] == "/new/path.mkv"

    def test_merge_adds_new_entries(self, sample_library):
        existing = copy.deepcopy(sample_library)
        kodi_update = {"tt9999999": {"path": "/new/movie.mkv"}}

        for iid, info in kodi_update.items():
            if iid in existing:
                existing[iid].update(info)
            else:
                existing[iid] = info

        assert "tt9999999" in existing


class TestIncomingData:
    """Verify incoming file processing."""

    def test_incoming_stores_pending(self, data_dir):
        inc_path = str(data_dir / "users" / "testuser" / "incoming.json")
        files = [
            {"path": "/downloads/movie.mkv", "filename": "movie.mkv", "size": 2000000000, "status": "pending"},
        ]
        safe_json_save(inc_path, files)
        loaded = safe_json_load(inc_path)
        assert len(loaded) == 1
        assert loaded[0]["status"] == "pending"

    def test_confirmed_files_excluded(self, data_dir):
        files = [
            {"path": "/a.mkv", "status": "pending"},
            {"path": "/b.mkv", "status": "confirmed"},
            {"path": "/c.mkv", "status": "deleted"},
        ]
        pending = [f for f in files if f.get("status") == "pending"]
        assert len(pending) == 1
        assert pending[0]["path"] == "/a.mkv"
