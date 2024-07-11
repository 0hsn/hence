"""Hence config test"""

import icecream
import pytest

import hence


class TestHenceConfig:
    """TestHenceConfig"""

    @staticmethod
    def test_context_add_pass():
        """test_context_add_pass"""

        hence.hence_config.context_add(hence.CTX_FN_BASE, {"some": {"var": 1}})
        ctx = hence.hence_config.context.get()

        assert hence.CTX_FN_BASE in ctx
        assert "some" in ctx[hence.CTX_FN_BASE]
        assert "var" in ctx[hence.CTX_FN_BASE]["some"]

    @staticmethod
    def test_context_add_fail():
        """test_context_add_fail"""

        with pytest.raises(TypeError) as te:
            hence.hence_config.context_add(hence.CTX_FN_BASE, [1, 2, 3])

        assert te.match("Only dict type supported for obj. found <class 'list'>")
