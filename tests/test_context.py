"""Test context related func"""

from hence import (
    CTX_FN_BASE,
    CTX_GR_BASE,
    CTX_TI_BASE,
    FuncConfig,
    GroupConfig,
    HenceConfig,
    TitleConfig,
    task,
)


class TestHenceConfigContextAdd:
    """Test HenceConfig.context_add"""

    @staticmethod
    def test_context_add_pass_for_func_config():
        """test context add pass"""

        hc = HenceConfig()
        hc.enable_log = True

        @task()
        def some_function(**kwargs):
            """some_function"""

        fc = FuncConfig(some_function, {}, sid="1")

        hc.context_add(fc)
        ctx_val = hc.context.get()

        assert "some_function.1" in ctx_val[CTX_FN_BASE]

    @staticmethod
    def test_context_add_pass_for_title_config():
        """test context add pass for TitleConfig"""

        hc = HenceConfig()
        hc.enable_log = True

        hc.context_add(TitleConfig("some_function", "some_function title"))
        ctx_val = hc.context.get()

        assert "some_function" in ctx_val[CTX_TI_BASE]

    @staticmethod
    def test_context_add_pass_for_group_config():
        """test context add pass for GroupConfig"""

        hc = HenceConfig()
        hc.enable_log = True

        hc.context_add(GroupConfig("abg", "some_function"))
        ctx_val = hc.context.get()
        assert "some_function" in ctx_val[CTX_GR_BASE]["abg"]

        hc.context_add(GroupConfig("abg", "some_function_2"))
        ctx_val = hc.context.get()
        assert "some_function_2" in ctx_val[CTX_GR_BASE]["abg"]


class TestHenceConfigContextGet:
    """TestHenceConfigContextGet"""

    @staticmethod
    def test_context_get_pass_for_base_node():
        """test context get pass for base node"""

        hc = HenceConfig()
        hc.enable_log = True

        hc.context_add(GroupConfig("abg", "some_function"))
        ctx_val = hc.context_get(CTX_GR_BASE)

        assert "abg" in ctx_val

    @staticmethod
    def test_context_get_pass_for_child_node():
        """test context get pass for child node"""

        hc = HenceConfig()
        hc.enable_log = True

        hc.context_add(GroupConfig("abg", "some_function_2"))
        ctx_val = hc.context_get(CTX_GR_BASE, "abg")

        assert "some_function_2" in ctx_val
