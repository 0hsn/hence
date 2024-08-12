"""Tests for Context"""

import pytest
from hence import (
    CTX_FN_BASE,
    CTX_GR_BASE,
    CTX_TI_BASE,
    GroupConfig,
    HenceContext,
    RunContext,
    TaskConfig,
    TitleConfig,
    _context,
    task,
)


class TestRunContext:
    """TestRunContext"""

    @staticmethod
    def test_setitem_fails_for_scaler():
        """test setitem fails for scaler"""

        rc = RunContext()

        with pytest.raises(TypeError):
            rc["some_key"] = 1

    @staticmethod
    def test_setitem_fails_for_collection():
        """test setitem fails for collection"""

        rc = RunContext()

        with pytest.raises(TypeError):
            rc["some_key"] = {"a": 1}

    @staticmethod
    def test_setitem_pass():
        """test setitem pass"""

        @task(title="")
        def sample(**kwargs): ...

        try:
            rc = RunContext()
            rc["some_key"] = TaskConfig(sample, {}, "0", "0")
        except TypeError as exc:
            assert False, f"'sum_x_y' raised an exception {exc}"

    @staticmethod
    def test_getitem_fails_with_keyerror():
        """test getitem pass returns null"""

        rc = RunContext()

        with pytest.raises(KeyError):
            assert rc["some_key"]

    @staticmethod
    def test_getitem_pass_returns_task_config():
        """test getitem pass returns task config"""

        @task(title="")
        def sample(**kwargs): ...

        rc = RunContext()
        rc["some_key"] = TaskConfig(sample, {}, "0", "0")

        assert isinstance(rc["some_key"], TaskConfig)


class TestHenceContextContextAdd:
    """Test HenceConfig.context_add"""

    @staticmethod
    def test_context_add_fail():
        """test_context_add_fail"""

        with pytest.raises(TypeError) as te:
            _context.context_add([1, 2, 3])

        assert te.match("context_add :: unsupported type")

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
