"""Tests for Pipeline"""

import pytest
from hence.init import Pipeline


class TestPipelineParameter:
    """Test Pipeline"""

    @staticmethod
    def test_parameter_fail():
        p = Pipeline()

        with pytest.raises(KeyError):
            p.parameter(a_param=21)

    @staticmethod
    def test_parameter_pass():
        p = Pipeline()
        p.context.sequence = ["a_param"]

        assert isinstance(p.parameter(a_param=21), Pipeline)
        assert p.context.parameters["a_param"] == 21
