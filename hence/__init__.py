"""
Hence
"""

from __future__ import annotations
from collections import UserDict
from contextvars import ContextVar
from functools import wraps, cached_property
import functools
from itertools import zip_longest
from types import FunctionType
import types
from typing import Any, NamedTuple, Protocol, Union
import typing
import uuid

import icecream
from loguru import logger
from paradag import DAG, SequentialProcessor, MultiThreadProcessor, dag_run
from pydantic import BaseModel, Field


def setup_dag(vertices: list) -> DAG:
    """Setup DAG"""

    _dag = DAG()

    _dag.add_vertex(*vertices)
    vertices_size = len(vertices)

    for index in range(1, vertices_size):
        _dag.add_edge(vertices[index - 1], vertices[index])

    return _dag


def execute_dag(
    dag: DAG,
    exc: ExecutorContract,
) -> list:
    """Execute the dag"""

    if not isinstance(dag, DAG):
        raise TypeError(f"Not a DAG. type: {type(dag)}")

    return dag_run(dag, processor=SequentialProcessor(), executor=exc)


class ExecutorContract(Protocol):
    """Interface for Executor"""

    def param(self, vertex) -> Any:
        """Have param"""

    def execute(self, __work) -> Any:
        """Can execute"""

    def report_finish(self, vertices_result):
        """Reports final steps"""


class FunctionExecutor:
    """Linear executor"""

    def __init__(self, ctx: PipelineContext):
        assert isinstance(ctx, PipelineContext)
        self.context: PipelineContext = ctx

    def param(self, vertex: Any) -> Any:
        """Selecting parameters"""

        return vertex

    def execute(self, uid: str) -> Any:
        """Execute node of the dag"""

        _function = self.context.functions[uid]
        _parameters = self.context.parameters[uid]

        self.context.result[uid] = _function(**_parameters)

    def report_finish(self, vertices_result: list):
        """deliver stores results"""


class PipelineContext(BaseModel):
    """Holds pipeline internal data"""

    result: dict[str, typing.Any] = Field(default_factory=dict)
    parameters: dict[str, dict[str, typing.Any]] = Field(default_factory=dict)
    sequence: list[str] = Field(default_factory=list)
    functions: dict[str, typing.Callable] = Field(default_factory=dict)


class Pipeline(BaseModel):
    """Base Pipeline utility class"""

    context: PipelineContext = Field(default_factory=PipelineContext)

    def add_task(
        self, uid: typing.Optional[str] = None, pass_ctx: bool = False
    ) -> typing.Any:
        """Add a task to pipeline"""

        def _internal(function: types.FunctionType):
            # if "kwargs" not in function.__code__.co_varnames:

            if pass_ctx and function.__code__.co_argcount == 0:
                raise AttributeError(
                    "pass_ctx is True, but function have no parameter."
                )

            first_param = function.__code__.co_varnames[0]

            if first_param in function.__annotations__:
                if not issubclass(
                    function.__annotations__[first_param],
                    PipelineContext,
                ):
                    raise AttributeError(
                        "If pass_ctx is True, function's 1st parameter MUST"
                        " have PipelineContext type annotation, or no type annotation"
                    )

            fn_name = uid if uid else function.__code__.co_name

            self.context.functions[fn_name] = function

            if fn_name not in self.context.sequence:
                self.context.sequence.append(fn_name)

            self.context.parameters[fn_name] = (
                {first_param: self.context} if pass_ctx else {}
            )

            @functools.wraps(function)
            def _decorator(**kwargs: dict) -> typing.Any:
                """decorator"""

                return function(**kwargs)

            return _decorator

        return _internal

    def re_add_task(self): ...

    def run(self) -> dict[str, typing.Any]:
        """Run a pipeline"""

        execute_dag(setup_dag(self.context.sequence), FunctionExecutor(self.context))

        return self.context.result

    def parameter(self, **kwargs) -> typing.Self:
        """Pass parameter to a task"""

        _keys = list(kwargs.keys())
        if len(_keys) > 1:
            raise AttributeError("Only one parameter is supported.")

        _key = _keys.pop(0)

        if _key not in self.context.sequence:
            raise KeyError(f"task uid `{_key}` not registered.")

        if not isinstance(kwargs[_key], dict):
            raise ValueError("Only pass a dict containing param name as key.")

        self.context.parameters[_key] |= kwargs[_key]

        return self
