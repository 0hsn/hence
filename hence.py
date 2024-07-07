"""
Hence
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from contextvars import ContextVar
import enum
from functools import wraps
from json import loads, dumps
import logging
import sys
from types import FunctionType
from typing import Any, Callable, Protocol, Union, final

from paradag import DAG, SequentialProcessor, MultiThreadProcessor, dag_run


CTX_NAME = "hence_context"
CTX_FN_BASE = "func"


class HenceConfig:
    """Hence configuration class"""

    enable_log: bool = False
    context: ContextVar[dict] = ContextVar(CTX_NAME, default={CTX_FN_BASE: {}})

    def logger_config(self):
        """loads or reloads HenceConfig"""

        stderr_log_formatter = logging.Formatter(
            "%(name)s :: %(levelname)s :: "
            + "(P)%(process)d/(Th)%(thread)d :: "
            + "%(message)s"
        )

        stdout_log_handler = logging.StreamHandler(stream=sys.stderr)
        stdout_log_handler.setLevel(logging.NOTSET)
        stdout_log_handler.setFormatter(stderr_log_formatter)

        logger.addHandler(stdout_log_handler)
        logger.setLevel(logging.DEBUG)

    def context_add(self, key: str, obj: dict):
        """Add to context"""

        if not isinstance(obj, dict):
            hence_log("error", "Only dict type supported for obj. found %s.", type(obj))
            raise TypeError(f"Only dict type supported for obj. found {type(obj)}")

        context_val = self.context.get()

        if CTX_FN_BASE in context_val:
            context_val[key] = context_val[key] | obj

    def context_search(self, key: str, obj_key: str):
        """Search in context"""

        context_val = self.context.get()

        if key in context_val:
            if obj_key in context_val[key]:
                return context_val[key][obj_key]

        hence_log("error", "Object with key: `%s` not found.", key)
        raise NotImplementedError(f"Object with key: `{key}` not found.")


logger = logging.getLogger("hence")

hence_config = HenceConfig()
hence_config.logger_config()


def hence_log(level: str, message: str, *args: list) -> None:
    """Final logging function"""
    if not hence_config.enable_log:
        return

    if level not in ("debug", "error"):
        raise SystemError("Invalid log type.")

    _log_level = logging.DEBUG if level == "debug" else logging.ERROR

    logger.log(_log_level, message, *args)


def get_step_out(args: dict, step_name: str) -> Any:
    """Gets step's returned output"""

    if not args or not isinstance(args, dict):
        raise TypeError("`args` is malformed.")

    if not step_name or not isinstance(step_name, str):
        raise TypeError("`step_name` is malformed.")

    if "__works__" not in args:
        raise TypeError("`args.__works__` do not exists.")

    if step_name not in args["__works__"]:
        raise TypeError(f"`{step_name}` not found in `args.__works__`.")

    return args["__works__"][step_name]


class WorkExecFrame:
    """WorkFrame holds what goes inside works"""

    OUT_KEY = "_result"

    def __init__(
        self,
        id_: str = "",
        title: str = "",
        function: Callable = lambda: ...,
        function_params: dict = None,
    ) -> None:
        """Create WorkExecFrame"""

        if not isinstance(id_, str):
            raise TypeError("String value expected for id_.")

        self._id = id_

        if not isinstance(title, str):
            raise TypeError("String value expected for title.")

        self._title: str = title

        if not isinstance(function, Callable):
            raise TypeError("Function must be a callable.")

        self._function: Callable = function

        if isinstance(self._function, AbstractWork):
            self._function_type = AbstractWork
        else:
            self._function_type = FunctionType

        self.function_params = function_params if function_params else {}
        self.function_out = ""

    @property
    def id(self) -> str:
        """get the id"""

        return self._id

    @property
    def function(self) -> Callable:
        """get the function"""

        return self._function

    @property
    def function_params(self) -> dict:
        """get the function"""

        return loads(self._function_params)

    @function_params.setter
    def function_params(self, val: dict) -> None:
        """get the function"""

        if not isinstance(val, dict):
            raise TypeError("Function params must be a dict.")

        self._function_params = dumps(val)

    @property
    def function_out(self) -> dict:
        """get the function output"""

        return loads(self._function_out).get(self.OUT_KEY, {})

    @function_out.setter
    def function_out(self, val: Any) -> None:
        """function_out setter"""

        self._function_out = dumps({self.OUT_KEY: val})

    def run(self, **kwargs):
        """run the function and save the result to output"""

        if len(kwargs) > 0 and not isinstance(kwargs, dict):
            raise TypeError("Function params must be a dict.")

        params = self.function_params | kwargs
        self.function_out = self.function(**params)

        return self.function_out


def task(title: str = None) -> list:
    """Task"""

    def _internal(function):
        """Internal handler"""

        hence_log("debug", "title `%s` registered.", title)
        # logger.debug("title `%s` registered.", title)

        if "kwargs" not in function.__code__.co_varnames:
            hence_log("error", "Missing %s(..., **kwargs).", type(function).__name__)
            raise TypeError(f"Missing {type(function).__name__}(..., **kwargs).")

        @wraps(function)
        def _decorator(**kwargs):
            """decorator"""

            hence_log("debug", "`%s` called with %s.", type(function).__name__, kwargs)
            return function(**kwargs)

        return _decorator

    return _internal




def setup_dag(vertices: list) -> DAG:
    """Setup DAG"""

    _dag = DAG()

    _dag.add_vertex(*vertices)

    for index in range(1, len(vertices)):
        _dag.add_edge(vertices[index - 1], vertices[index])

    return _dag
def work(
    before: Callable = lambda: ...,
    after: Callable = lambda: ...,
):
    """work"""

    def inner(func):
        """inner"""

        if "kwargs" not in func.__code__.co_varnames:
            raise TypeError(f"Missing {type(func).__name__}(..., **kwargs).")

        @wraps(func)
        def decorator(**kwargs):
            """decorator"""

            kwargs["__before__"] = before()
            returnable = func(**kwargs)
            after()

            return returnable

        return decorator

    return inner


class AbstractWork(ABC):
    """Base work type"""

    def __init__(self) -> None:
        """Constructor"""

        self._name = type(self).__name__

    def __before__(self) -> Any:
        """default before impl"""

        return Ellipsis

    def __after__(self) -> Any:
        """default after impl"""

        return Ellipsis

    @abstractmethod
    def __work__(self, **kwargs):
        "Force implement function"

        raise NotImplementedError("__work__ not implemented.")

    def __call__(self, **kwargs):
        kwargs["__before__"] = self.__before__()
        returnable = self.__work__(**kwargs)
        self.__after__()

        return returnable


class DagExecutionType(enum.IntEnum):
    """Dag execution type

    Args:
        enum (Sequential): Select for sequential processing
        enum (Parallel): Select for parallel processing
    """

    SEQUENTIAL = 0
    PARALLEL = 1


ProcessorType = SequentialProcessor | MultiThreadProcessor
"""Processor type: paradag.SequentialProcessor or paradag.MultiThreadProcessor"""


class DagExecutor:
    """DagExecutor"""

    def __init__(self, proc: DagExecutionType = DagExecutionType.SEQUENTIAL) -> None:
        """DagExecutor constructor"""

        self._dag = DAG()

        if not isinstance(proc, DagExecutionType):
            raise ValueError("Unsupported dag execution type")

        self._processor = (
            SequentialProcessor()
            if proc == DagExecutionType.SEQUENTIAL
            else MultiThreadProcessor()
        )

    @property
    @abstractmethod
    def vertices(self) -> list[Any]:
        """Get unit_of_works"""

    @property
    def processor(self) -> ProcessorType:
        """Get the processor"""

        return self._processor

    @final
    def setup_dag(self) -> bool:
        """Setup DAG"""

        self._dag.add_vertex(*self.vertices)

        for index in range(1, len(self.vertices)):
            self._dag.add_edge(self.vertices[index - 1], self.vertices[index])

    @final
    def execute_dag(self) -> list[DagExecutor]:
        """Execute the dag"""

        resp = dag_run(
            self._dag,
            processor=self.processor,
            executor=LinearExecutor(),
        )

        return resp


class WorkGroup(DagExecutor):
    """Collection of Work"""

    def __init__(self, wef_list: list[WorkExecFrame]) -> None:
        """Constructor"""

        super().__init__()

        self._name = type(self).__name__

        self._works = []

        list(map(self.append, wef_list))

        if self._works:
            self.setup_dag()

    @property
    def vertices(self) -> list[WorkExecFrame]:
        return self._works if self._works else []

    def append(self, wef: WorkExecFrame) -> bool:
        """Append a WorkExecFrame to the Workgroup"""

        self._works.append(self._validate_type(wef))

    def _validate_type(self, value):
        """Validate values before appending"""

        if not isinstance(value, WorkExecFrame):
            raise TypeError(f"WorkExecFrame expected, got {type(value).__name__}.")

        if not isinstance(value.function, (AbstractWork, FunctionType)):
            raise TypeError(
                f"Function of type AbstractWork or FunctionType expected, got {type(value).__name__}."
            )

        if (
            isinstance(value.function, AbstractWork)
            and "kwargs" not in value.function.__work__.__code__.co_varnames
        ):
            raise TypeError(
                f"Missing {type(value.function).__name__}.__work__(..., **kwargs)."
            )

        if (
            isinstance(value.function, FunctionType)
            and value.function.__code__.co_name != "decorator"
        ):
            raise TypeError("Unsupported work found. @work() decorated expected.")

        return value


class Workflow(DagExecutor):
    """Base workflow type"""

    def __init__(self, work_groups: list[WorkGroup] = None) -> None:
        """Constructor"""

        super().__init__(DagExecutionType.PARALLEL)

        self._name = type(self).__name__

        self._work_groups: list[WorkGroup] = (
            work_groups if work_groups and self.__validate(work_groups) else []
        )

        self.setup_dag()

    @property
    def vertices(self) -> list[WorkGroup]:
        return self._work_groups if self._work_groups else []

    @staticmethod
    def __validate(wgs: list[WorkGroup]) -> bool:
        """Validate tasks are ok"""

        if not all([isinstance(wg, WorkGroup) for wg in wgs]):
            raise TypeError("Unsupported workgroup found.")

        return True


class ExecutorContract(Protocol):
    """Interface for Executor"""

    def param(self, vertex) -> Any:
        """Have param"""

    def execute(self, __work) -> Any:
        """Can execute"""

    def report_finish(self, vertices_result):
        """Reports final steps"""


class LinearExecutor:
    """Linear executor"""

    RES_KEY = "__works__"

    def __init__(self) -> None:
        """init LinearExecutor"""

        self._results = {}

    def param(self, vertex: Any) -> Any:
        """Selecting parameters"""

        return vertex

    def execute(self, __work: WorkExecFrame | WorkGroup) -> Any:
        """Execute"""

        if isinstance(__work, WorkExecFrame) and callable(__work.function):
            return __work.run(**{self.RES_KEY: self._results})
        elif isinstance(__work, WorkGroup):
            return __work.execute_dag()
        else:
            raise TypeError(f"Incorrect type of `work` {type(__work)} found.")

    def report_finish(self, vertices_result):
        """After execution finished"""

        for vertex, result in vertices_result:
            if not isinstance(vertex, WorkGroup) and len(vertex.id) > 0:
                self._results[vertex.id] = result


class FunctionTypeExecutor:
    """Linear executor"""

    RES_KEY = "__works__"

    def __init__(self) -> None:
        """init LinearExecutor"""

        self._results = {}

    def param(self, vertex: Any) -> Any:
        """Selecting parameters"""

        return vertex

    def execute(self, task_: FunctionType) -> Any:
        """Execute"""

        t_info = hence_config.context_search(CTX_FN_BASE, task_.__name__)

        t_title = t_info["title"] if "title" in t_info else task_.__name__
        t_params = t_info["parameters"] if "parameters" in t_info else {}

        hence_log("debug", "`%s` is executing.", t_title)

        return task_(**t_params)
