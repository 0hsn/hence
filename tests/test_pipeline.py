"""Tests for Pipeline"""

import pytest
from hence import Pipeline, PipelineContext


class TestPipelineParameter:
    """Test Pipeline"""

    @staticmethod
    def test_parameter_fail():
        p = Pipeline()

        with pytest.raises(KeyError):
            p.parameter(a_param={"var": 21})

    @staticmethod
    def test_parameter_pass():
        p = Pipeline()

        @p.add_task()
        def a_param(): ...

        assert isinstance(p.parameter(a_param={"var": 21}), Pipeline)
        assert p.context.parameters["a_param"] == {"var": 21}

    @staticmethod
    def test_fails_for_unregister_function():
        pipeline = Pipeline()

        @pipeline.add_task(pass_ctx=True)
        def function_1(ctx, a: str):
            return function_1.__name__

        with pytest.raises(KeyError):
            pipeline.parameter(function_2={"a": "String"})

    @staticmethod
    def test_pass_for_register_function():
        pipeline = Pipeline()

        @pipeline.add_task()
        def function_1(a: str):
            return a

        pipeline.parameter(function_1={"a": "String"})

        assert pipeline.context.parameters["function_1"] == {"a": "String"}


class TestPipelineAddTask:
    @staticmethod
    def test_fail_for_no_ctx_var():
        pipeline = Pipeline()

        with pytest.raises(AttributeError):

            @pipeline.add_task(pass_ctx=True)
            def function_1():
                return function_1.__name__

    @staticmethod
    def test_fail_for_misleading_type():
        pipeline = Pipeline()

        with pytest.raises(AttributeError):

            @pipeline.add_task(pass_ctx=True)
            def function_1(ctx: int, a: str):
                return function_1.__name__

    @staticmethod
    def test_pass_for_correct_context_type():
        pipeline = Pipeline()

        @pipeline.add_task(pass_ctx=True)
        def function_1(ctx: PipelineContext, a: str):
            return function_1.__name__

    @staticmethod
    def test_pass_for_no_context_type():
        pipeline = Pipeline()

        @pipeline.add_task(pass_ctx=True)
        def function_1(ctx, a: str):
            return function_1.__name__


class TestPipelineRun:
    @staticmethod
    def test_pass(capsys):
        pipeline = Pipeline()

        @pipeline.add_task(pass_ctx=True)
        def function_1(ctx, a: str):
            return a

        @pipeline.add_task(pass_ctx=True)
        def function_2(ctx, b: str):
            print(b)

        pipeline.parameter(function_1={"a": "String"})
        pipeline.parameter(function_2={"b": "StringB"})

        result = pipeline.run()
        captured = capsys.readouterr()

        assert len(result) == 2
        assert result["function_1"] == "String"
        assert not result["function_2"]
        assert "StringB" in captured.out
