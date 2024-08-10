"""Tests for Context"""

import pytest
from hence import RunLevelContext, TaskConfig, task


class TestRunLevelContext:
    """TestRunLevelContext"""

    @staticmethod
    def test_setitem_fails_for_scaler():
        """test setitem fails for scaler"""

        rc = RunLevelContext()

        with pytest.raises(TypeError):
            rc["some_key"] = 1

    @staticmethod
    def test_setitem_fails_for_collection():
        """test setitem fails for collection"""

        rc = RunLevelContext()

        with pytest.raises(TypeError):
            rc["some_key"] = {"a": 1}

    @staticmethod
    def test_setitem_pass():
        """test setitem pass"""

        @task(title="")
        def sample(**kwargs): ...

        try:
            rc = RunLevelContext()
            rc["some_key"] = TaskConfig(sample, {}, "0", "0")
        except TypeError as exc:
            assert False, f"'sum_x_y' raised an exception {exc}"

    @staticmethod
    def test_getitem_pass_returns_null():
        """test getitem pass returns null"""

        rc = RunLevelContext()

        assert rc["some_key"] is None

    @staticmethod
    def test_getitem_pass_returns_task_config():
        """test getitem pass returns task config"""

        @task(title="")
        def sample(**kwargs): ...

        rc = RunLevelContext()
        rc["some_key"] = TaskConfig(sample, {}, "0", "0")

        assert isinstance(rc["some_key"], TaskConfig)

    @staticmethod
    def test_step_pass_returns_task_config():
        """test step pass returns task config"""

        @task(title="")
        def sample(**kwargs): ...

        tc = TaskConfig(sample, {}, "0", "0")

        rc = RunLevelContext()
        rc[tc.task_key] = tc

        assert isinstance(rc.step(0), TaskConfig)

    @staticmethod
    def test_step_pass_returns_none():
        """test step pass returns none"""

        rc = RunLevelContext()

        assert rc.step(0) is None

    @staticmethod
    def test_step_fails_when_assigned_same_index():
        """test step fails when assigned same index"""

        @task(title="")
        def sample(**kwargs): ...

        tc = TaskConfig(sample, {}, "0", "0")

        rc = RunLevelContext()
        rc[tc.task_key] = tc

        with pytest.raises(ValueError):
            rc[tc.task_key] = tc

    @staticmethod
    def test_step_passes_when_key_del():
        """test step passes when key del"""

        @task(title="")
        def sample(**kwargs): ...

        tc = TaskConfig(sample, {}, "0", "0")

        rc = RunLevelContext()
        rc[tc.task_key] = tc

        del rc[tc.task_key]

        try:
            rc[tc.task_key] = tc
        except ValueError as exc:
            assert False, f"'sum_x_y' raised an exception {exc}"
