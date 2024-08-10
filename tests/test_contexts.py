"""Tests for Context"""

import pytest
import icecream
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
