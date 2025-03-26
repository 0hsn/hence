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


CTX_NAME = "hence_context"
CTX_FN_BASE = "func"
CTX_GR_BASE = "group"
CTX_RL_BASE = "runs"
CTX_TI_BASE = "title"

# logger setup
log_debug = logger.debug
log_info = logger.info
log_error = logger.error


class TitleConfig(NamedTuple):
    """TitleConfig"""

    task_key: str
    title: str


class GroupConfig(NamedTuple):
    """GroupConfig"""

    title: str
    function: FunctionType


class TaskConfig:
    """TaskConfig"""

    def __init__(
        self, fn: FunctionType, params: dict, rid: str = "", sid: str = ""
    ) -> None:
        """constructor"""

        if not sid:
            raise ValueError("Sequence id empty.")

        _title = _context.context_get(CTX_TI_BASE, fn.__name__)

        self.function: FunctionType = fn
        self.parameters: dict = params
        self.run_id: str = rid
        self.seq_id: str = sid
        self.title: str = _title if _title else fn.__name__
        self.result: Any = None

    @cached_property
    def task_key(self) -> str:
        """Make a context key for functions"""

        if not self.function:
            raise ValueError("function is empty.")

        task_key_ = f"{self.seq_id}"

        if self.run_id:
            task_key_ += "." + self.run_id

        return task_key_

    def asdict(self) -> dict:
        """asdict"""

        return self.__dict__

    def __repr__(self) -> str:
        """__repr__"""

        return str(self.asdict())

    def format_title(self) -> bool:
        """format title"""

        if "{" in self.title and "}" in self.title:
            t_title: str = self.title

            try:
                t_title = t_title.format(
                    fn_task_key=self.task_key,
                    fn_name=self.function.__name__,
                    fn_run_id=self.run_id,
                    fn_seq_id=self.seq_id,
                )
            except KeyError as e:
                log_error(f"`{e}` not found in task.title.")
                raise e

            self.title = t_title
            return True
        return False

    @staticmethod
    def from_task_key(t_key: str) -> "TaskConfig":
        """Create a TaskConfig from task_key"""
        _, run_context_id = t_key.split(".", 1)

        rlc: RunContext = RunContextSupport.from_context(run_context_id)
        return rlc[t_key]


class RunContext(UserDict):
    """Context data for Group and bloc of Task"""

    def __setitem__(self, key: str, item: TaskConfig) -> None:
        """Add a TaskConfig to data"""

        if not isinstance(item, TaskConfig):
            raise TypeError("A TaskConfig for given.")

        super().__setitem__(key, item)


class RunContextSupport:
    """RunContext Support"""

    @staticmethod
    def from_context(run_context_id: str) -> "RunContext":
        """Create from context"""

        r_ctx = _context.context_get(CTX_RL_BASE, run_context_id)

        if not isinstance(r_ctx, RunContext):
            raise ValueError(f"RunContext not found for: `{run_context_id}`")

        return r_ctx

    @staticmethod
    def to_context(r_ctx: RunContext, run_context_id: str) -> None:
        """Save to context"""

        _context.context_add(r_ctx, run_context_id=run_context_id)


class Utils:
    """Utils to get state data"""

    @staticmethod
    def get_step(seq_id: int, run_id: str) -> TaskConfig:
        """Get a task by seq_id"""

        return TaskConfig.from_task_key(f"{seq_id}.{run_id}")

    @staticmethod
    def get_task(task_key: str) -> TaskConfig:
        """Get a task by task_key"""

        return TaskConfig.from_task_key(task_key)


class HenceContext:
    """Hence configuration class"""

    def __init__(self) -> None:
        """Constructor"""

        self.context: ContextVar[dict] = None

        self._setup_context()

    def _setup_context(self) -> None:
        """Setup contextvar"""
        del self.context

        self.context: ContextVar[dict] = ContextVar(
            CTX_NAME,
            default={
                CTX_GR_BASE: {},
                CTX_RL_BASE: {},
                CTX_TI_BASE: {},
            },
        )

    def context_add(
        self,
        obj: TaskConfig | GroupConfig | TitleConfig | RunContext,
        /,
        **kwargs: Any,
    ) -> None:
        """Add to context"""

        if not isinstance(
            obj,
            (
                GroupConfig,
                TitleConfig,
                RunContext,
            ),
        ):
            raise TypeError("context_add :: unsupported type")

        if isinstance(obj, TitleConfig):
            context_val = self.context.get()
            context_val[CTX_TI_BASE][obj.task_key] = obj.title

            log_debug(f"TitleConfig: obj = {obj}.")

        elif isinstance(obj, GroupConfig):
            context_val = self.context.get()

            if obj.title not in context_val[CTX_GR_BASE]:
                context_val[CTX_GR_BASE][obj.title] = [obj.function]
            else:
                context_val[CTX_GR_BASE][obj.title].append(obj.function)

            log_debug(f"GroupConfig: obj = {obj}.")

        elif isinstance(obj, RunContext):
            context_val = self.context.get()

            if "run_context_id" in kwargs:
                context_val[CTX_RL_BASE][kwargs["run_context_id"]] = obj
                log_debug(f"RunContext: obj = {obj}.")
            else:
                log_error("`kwargs[run_context_id]` not found.")
                raise ValueError("`run_context_id` not found.")

        log_info("Add context successful.")

    def context_get(self, key: str, obj_key: str = "") -> Any:
        """Get from context"""

        context_val = self.context.get()

        if key not in context_val:
            log_error(f"Object with key: `{key}` not found.")
            raise KeyError(f"Object with key: `{key}` not found.")

        if obj_key and obj_key not in context_val[key]:
            log_error(f"Object with key: `{obj_key}` not found.")
            raise KeyError(f"Object with key: `{obj_key}` not found.")

        ret_value = context_val[key][obj_key] if obj_key else context_val[key]
        log_debug(f" :: context_get ::`{ret_value}` for {key}.{obj_key}.")

        return ret_value


_context = HenceContext()


def group(group_id: str) -> Any:
    """Group"""

    group_lst = _context.context_get(CTX_GR_BASE)

    if group_id in group_lst:
        log_error(f"`{group_id}` exists already.")
        raise ValueError(f"`{group_id}` exists already.")

    def _internal(fn: FunctionType):
        """Internal handler"""

        _context.context_add(GroupConfig(title=group_id, function=fn))

    return _internal


def task(title: str = None) -> Any:
    """Task"""

    def _internal(function: FunctionType):
        """Internal handler"""

        t_title = title if title else function.__name__

        # save function title to context
        log_debug(f"title `{t_title}` registered.")

        t_conf = TitleConfig(function.__name__, t_title)
        _context.context_add(t_conf)

        if "kwargs" not in function.__code__.co_varnames:
            log_error(f"Missing `{function.__name__}(.., **kwargs)`.")
            raise TypeError(f"Missing `{function.__name__}(.., **kwargs)`.")

        @wraps(function)
        def _decorator(**kwargs: dict) -> Any:
            """decorator"""

            log_debug(f"`{function.__name__}` called with {kwargs}.")
            return function(**kwargs)

        return _decorator

    return _internal


def run_tasks(fn_config_list: list[tuple], run_id: str = "") -> list[str]:
    """Run @task"""

    fn_list = []
    rl_context = RunContext()

    if len(run_id) == 0:
        run_id = str(uuid.uuid4())

    for index, fn_config_tpl in enumerate(fn_config_list):
        log_debug(f"`run_tasks` :: {fn_config_tpl}")

        if len(fn_config_tpl) > 2:
            log_error("Not allowed extra values in `fn_config_list`")
            raise ValueError("Not allowed extra values in `fn_config_list`")

        fn_config = TaskConfig(sid=str(index), rid=run_id, *fn_config_tpl)

        rl_context[fn_config.task_key] = fn_config
        _context.context_add(rl_context, run_context_id=run_id)

        fn_list.append(fn_config.task_key)

    if not fn_list:
        log_error("`fn_list` does not contain any `@task`.")
        raise TypeError("`fn_list` does not contain any `@task`.")

    return execute_dag(setup_dag(fn_list), FunctionTypeExecutor())


def run_group(group_id: str, task_params: list[dict]) -> Any:
    """Run groups"""

    groups = _context.context_get(CTX_GR_BASE)

    if group_id not in groups:
        raise RuntimeError(f"group `{group_id}` is not found.")

    group_params = list(
        zip_longest(
            groups[group_id],
            task_params,
            fillvalue={},
        )
    )

    return run_tasks(group_params, group_id)


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


class FunctionTypeExecutor:
    """Linear executor"""

    def param(self, vertex: Any) -> Any:
        """Selecting parameters"""

        return vertex

    def execute(self, task_key: str) -> Any:
        """Execute"""

        curr_step, run_context_id = task_key.split(".", 1)

        rlc: RunContext = RunContextSupport.from_context(run_context_id)
        task_cfg: TaskConfig = rlc[task_key]

        if task_cfg.format_title():
            rlc[task_cfg.task_key] = task_cfg
            RunContextSupport.to_context(rlc, run_context_id)

        log_debug(f"`{task_cfg.title}::{task_cfg.task_key}` is executing.")

        task_cfg.parameters |= {
            "_META_": {"run_id": run_context_id, "current_step": curr_step}
        }

        return task_cfg.function(**task_cfg.parameters)

    def report_finish(self, vertices_result: list):
        """deliver stores results"""

        task_key, result = vertices_result[0]
        _, run_context_id = task_key.split(".", 1)

        rlc: RunContext = RunContextSupport.from_context(run_context_id)
        task_cfg: TaskConfig = rlc[task_key]

        task_cfg.result = result

        rlc[task_cfg.task_key] = task_cfg
        RunContextSupport.to_context(rlc, run_context_id)


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
