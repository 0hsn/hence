"""Tests for Context"""

import pytest
from hence import (
    CTX_FN_BASE,
    CTX_GR_BASE,
    CTX_TI_BASE,
    GroupConfig,
    HenceContext,
    RunLevelContext,
    TaskConfig,
    TitleConfig,
    task,
)


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


class TestHenceContextContextAdd:
    """Test HenceConfig.context_add"""

    @staticmethod
    def test_context_add_pass_for_func_config():
        """test context add pass"""

        hc = HenceContext()

        @task()
        def some_function(**kwargs):
            """some_function"""

        fc = TaskConfig(some_function, {}, sid="1")

        hc.context_add(fc)
        ctx_val = hc.context.get()

        assert "1" in ctx_val[CTX_FN_BASE]

    @staticmethod
    def test_context_add_pass_for_title_config():
        """test context add pass for TitleConfig"""

        hc = HenceContext()

        hc.context_add(TitleConfig("some_function", "some_function title"))
        ctx_val = hc.context.get()

        assert "some_function" in ctx_val[CTX_TI_BASE]

    @staticmethod
    def test_context_add_pass_for_group_config():
        """test context add pass for GroupConfig"""

        hc = HenceContext()

        @task(title="")
        def some_function(**kwargs): ...

        @task(title="")
        def some_function_2(**kwargs): ...

        hc.context_add(GroupConfig("abg", some_function))
        ctx_val = hc.context.get()
        assert some_function in ctx_val[CTX_GR_BASE]["abg"]

        hc.context_add(GroupConfig("abg", some_function_2))
        ctx_val = hc.context.get()
        assert some_function_2 in ctx_val[CTX_GR_BASE]["abg"]


class TestHenceContextContextGet:
    """TestHenceContextContextGet"""

    @staticmethod
    def test_context_get_pass_for_base_node():
        """test context get pass for base node"""

        hc = HenceContext()

        @task(title="")
        def some_function(**kwargs): ...

        hc.context_add(GroupConfig("abg", some_function))
        ctx_val = hc.context_get(CTX_GR_BASE)

        assert "abg" in ctx_val

    @staticmethod
    def test_context_get_pass_for_child_node():
        """test context get pass for child node"""

        hc = HenceContext()

        @task(title="")
        def some_function(**kwargs): ...

        hc.context_add(GroupConfig("abg", some_function))
        ctx_val = hc.context_get(CTX_GR_BASE, "abg")

        assert some_function in ctx_val
