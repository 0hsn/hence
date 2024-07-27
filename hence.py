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

from immutabledict import immutabledict
from paradag import DAG, SequentialProcessor, MultiThreadProcessor, dag_run


CTX_NAME = "hence_context"
CTX_FN_BASE = "func"
CTX_TI_BASE = "title"


class HenceConfig:
    """Hence configuration class"""

    def __init__(self) -> None:
        """Constructor"""

        self.enable_log: bool = False
        self._logger_config()

        self.context: ContextVar[dict] = ContextVar(CTX_NAME, default={CTX_FN_BASE: {}})

    def _logger_config(self):
        """Loads or reloads HenceConfig"""

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

    def context_add(self, key: str, obj: FuncConfig) -> None:
        """Add to context"""

        if not isinstance(obj, dict):
            hence_log("error", "Only dict type supported for obj. found %s.", type(obj))
            raise TypeError(f"Only dict type supported for obj. found {type(obj)}")

        context_val = self.context.get()

        context_val[key] = context_val[key] | obj if key in context_val else obj
        hence_log("debug", "Context:: %s.", self.context)

    def context_search(self, key: str, obj_key: str) -> Any:
        """Search in context"""

        context_val = self.context.get()

        if key not in context_val:
            hence_log("error", "Object with key: `%s` not found.", key)
            raise KeyError(f"Object with key: `{key}` not found.")

        if obj_key not in context_val[key]:
            hence_log("error", "Object with key: `%s` not found.", obj_key)
            raise KeyError(f"Object with key: `{obj_key}` not found.")

        hence_log(
            "debug",
            "returning : `%s` for %s.%s .",
            context_val[key][obj_key],
            key,
            obj_key,
        )

        return context_val[key][obj_key]

    def task(self, obj_key: str) -> FuncConfig:
        """Get a task by key"""

        return self.context_search(CTX_FN_BASE, obj_key)


logger = logging.getLogger("hence")

hence_config = HenceConfig()


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


def task(title: str = None) -> Any:
    """Task"""

    def _internal(function: FunctionType):
        """Internal handler"""

        t_title = title if title else function.__name__

        # save function title to context
        hence_log("debug", "title `%s` registered.", t_title)
        hence_config.context_add(CTX_TI_BASE, {function.__name__: t_title})

        if "kwargs" not in function.__code__.co_varnames:
            hence_log("error", "Missing `**kwargs` in %s args.", function.__name__)
            raise TypeError(f"Missing `**kwargs` in {function.__name__} args.")

        @wraps(function)
        def _decorator(**kwargs: dict) -> Any:
            """decorator"""

            hence_log("debug", "`%s` called with %s.", function.__name__, kwargs)
            return function(**kwargs)

        return _decorator

    return _internal


class FuncConfig:
    """FuncConfig"""

    def __init__(
        self, fn: FunctionType, params: dict, rid: str = "", sid: str = ""
    ) -> None:
        """constructor"""

        if not sid:
            raise ValueError("Sequence id empty.")

        _title = hence_config.context_search(CTX_TI_BASE, fn.__name__)

        self.function: FunctionType = fn
        self.parameters: immutabledict = immutabledict(params)
        self.run_id: str = rid
        self.seq_id: str = sid
        self.title: str = _title if _title else fn.__name__
        self.result: Any = None

    @property
    def task_key(self) -> str:
        """Make a context key for functions"""

        if not self.function:
            raise ValueError("function is empty.")

        task_key_ = f"{self.function.__name__}.{self.seq_id}"

        if self.run_id:
            task_key_ += "." + self.run_id

        return task_key_

    def asdict(self) -> dict:
        """asdict"""

        result = self.__dict__
        if "parameters" in result:
            result["parameters"] = dict(result["parameters"])

        return result

    def __repr__(self) -> str:
        """__repr__"""

        return str(self.asdict())


def run_tasks(fn_config_list: list[tuple]) -> list[FunctionType]:
    """Run @task"""

    fn_list = []

    for index, fn_config_tpl in enumerate(fn_config_list):

        if len(fn_config_tpl) > 2:

            raise ValueError(
                f"Only function and parameters are allowed in `{run_tasks.__name__}`"
            )

        fn_config = FuncConfig(sid=str(index), *fn_config_tpl)

        hence_log("debug", "`run_tasks` :: %s", fn_config_tpl)

        hence_config.context_add(CTX_FN_BASE, {fn_config.task_key: fn_config})

        fn_list.append(fn_config.task_key)

    if not fn_list:
        hence_log("error", "`fn_list` does not contain any `@task`.")
        raise TypeError("`fn_list` does not contain any `@task`.")

    _dag = setup_dag(fn_list)
    return execute_dag(_dag, SequentialProcessor(), FunctionTypeExecutor())


def setup_dag(vertices: list) -> DAG:
    """Setup DAG"""

    _dag = DAG()

    _dag.add_vertex(*vertices)

    for index in range(1, len(vertices)):
        _dag.add_edge(vertices[index - 1], vertices[index])

    return _dag


def execute_dag(
    dag: DAG,
    processor_: Union[SequentialProcessor, MultiThreadProcessor],
    executor_: ExecutorContract,
) -> list[FunctionType]:
    """Execute the dag"""

    if not isinstance(dag, DAG):
        raise TypeError(f"Not a DAG. type: {type(dag)}")

    return dag_run(dag, processor=processor_, executor=executor_)


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

    def execute(self, task_key: str) -> Any:
        """Execute"""

        fn_cfg: FuncConfig = hence_config.context_search(CTX_FN_BASE, task_key)

        t_title: str = fn_cfg.title

        # replace supported variables in title
        if "{" in t_title and "}" in t_title:

            try:
                t_title = t_title.format(
                    fn_key=fn_cfg.task_key,
                    fn_name=fn_cfg.function.__name__,
                    fn_run_id=fn_cfg.run_id,
                    fn_seq_id=fn_cfg.seq_id,
                )
            except KeyError as e:
                hence_log("error", "`%s` not found in task.title.", str(e))
                raise e

            fn_cfg.title = t_title
            hence_config.context_add(CTX_FN_BASE, {task_key: fn_cfg})

        hence_log("debug", "`%s::%s` is executing.", t_title, task_key)

        return fn_cfg.function(**fn_cfg.parameters)

    def report_finish(self, vertices_result: list):
        """After execution finished"""

        fn_key, fn_result = vertices_result[0]

        fn_obj: FuncConfig = hence_config.context_search(CTX_FN_BASE, fn_key)
        fn_obj.result = fn_result

        hence_config.context_add(CTX_FN_BASE, {fn_key: fn_obj})
