"""Hence config test"""

import pytest

import hence


class TestHenceConfig:
    """TestHenceConfig"""

    @staticmethod
    def test_context_add_pass():
        """test_context_add_pass"""

        @hence.task()
        def one_task(**kwargs): ...

        _fc = hence.TaskConfig(one_task, {}, sid="1")

        hence._context.context_add(_fc)
        ctx = hence._context.context.get()

        assert hence.CTX_FN_BASE in ctx
        assert "1" in ctx[hence.CTX_FN_BASE]

    @staticmethod
    def test_context_add_fail():
        """test_context_add_fail"""

        with pytest.raises(TypeError) as te:
            hence._context.context_add([1, 2, 3])

        assert te.match("context_add :: unsupported type")
