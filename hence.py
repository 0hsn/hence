"""
Hence
"""

from __future__ import annotations
from contextvars import ContextVar
from functools import wraps, cached_property
from itertools import zip_longest
import logging
import sys
from types import FunctionType
from typing import Any, NamedTuple, Protocol, Union

from immutabledict import immutabledict
from paradag import DAG, SequentialProcessor, MultiThreadProcessor, dag_run


CTX_NAME = "hence_context"
CTX_FN_BASE = "func"
CTX_TI_BASE = "title"
CTX_GR_BASE = "group"


class TitleConfig(NamedTuple):
    """TitleConfig"""

    task_key: str
    title: str


class GroupConfig(NamedTuple):
    """GroupConfig"""

    title: str
    function: FunctionType


class FuncConfig:
    """FuncConfig"""

    def __init__(
        self, fn: FunctionType, params: dict, rid: str = "", sid: str = ""
    ) -> None:
        """constructor"""

        if not sid:
            raise ValueError("Sequence id empty.")

        _title = hence_config.context_get(CTX_TI_BASE, fn.__name__)

        self.function: FunctionType = fn
        self.parameters: immutabledict = immutabledict(params)
        self.run_id: str = rid
        self.seq_id: str = sid
        self.title: str = _title if _title else fn.__name__
        self.result: Any = None

    @cached_property
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


class HenceConfig:
    """Hence configuration class"""

    def __init__(self) -> None:
        """Constructor"""

        self.enable_log: bool = False
        self.context: ContextVar[dict] = None

        self._setup_logger()
        self._setup_context()

    def _setup_context(self) -> None:
        """Setup contextvar"""
        del self.context

        self.context: ContextVar[dict] = ContextVar(
            CTX_NAME,
            default={
                CTX_FN_BASE: {},
                CTX_GR_BASE: {},
                CTX_TI_BASE: {},
            },
        )

    def _setup_logger(self) -> None:
        """Loads or reloads logger"""

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

    def context_add(self, obj: FuncConfig | GroupConfig | TitleConfig) -> None:
        """Add to context"""

        if not isinstance(obj, (FuncConfig, GroupConfig, TitleConfig)):
            raise TypeError("context_add :: unsupported type")

        if isinstance(obj, FuncConfig):
            context_val = self.context.get()
            context_val[CTX_FN_BASE][obj.task_key] = obj

        elif isinstance(obj, TitleConfig):
            context_val = self.context.get()
            context_val[CTX_TI_BASE][obj.task_key] = obj.title

        elif isinstance(obj, GroupConfig):
            context_val = self.context.get()

            if obj.title not in context_val[CTX_GR_BASE]:
                context_val[CTX_GR_BASE][obj.title] = [obj.function]
            else:
                context_val[CTX_GR_BASE][obj.title].append(obj.function)

        hence_log("debug", "Context:: %s.", self.context)

    def context_get(self, key: str, obj_key: str = "") -> Any:
        """Get from context"""

        context_val = self.context.get()

        if key not in context_val:
            hence_log("error", "Object with key: `%s` not found.", key)
            raise KeyError(f"Object with key: `{key}` not found.")

        if obj_key and obj_key not in context_val[key]:
            hence_log("error", "Object with key: `%s` not found.", obj_key)
            raise KeyError(f"Object with key: `{obj_key}` not found.")

        ret_value = context_val[key][obj_key] if obj_key else context_val[key]

        hence_log("debug :: context_get ::", "`%s` for %s.%s.", ret_value, key, obj_key)

        return ret_value

    def task(self, obj_key: str) -> FuncConfig:
        """Get a task by key"""

        return self.context_get(CTX_FN_BASE, obj_key)


logger = logging.getLogger("hence")

hence_config = HenceConfig()


def hence_log(level: str, message: str, *args) -> None:
    """Final logging function"""
    if not hence_config.enable_log:
        return

    if level not in ("debug", "error"):
        raise SystemError("Invalid log type.")

    _log_level = logging.DEBUG if level == "debug" else logging.ERROR

    logger.log(_log_level, message, *args)


def group(group_id: str) -> Any:
    """Group"""

    group_lst = hence_config.context_get(CTX_GR_BASE)

    if group_id in group_lst:
        raise ValueError(f"`{group_id}` exists already.")

    def _internal(fn: FunctionType):
        """Internal handler"""

        hence_config.context_add(GroupConfig(title=group_id, function=fn))

    return _internal


def task(title: str = None) -> Any:
    """Task"""

    def _internal(function: FunctionType):
        """Internal handler"""

        t_title = title if title else function.__name__

        # save function title to context
        hence_log("debug", "title `%s` registered.", t_title)

        t_conf = TitleConfig(function.__name__, t_title)
        hence_config.context_add(t_conf)

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


def run_tasks(fn_config_list: list[tuple], run_id: str = "") -> list[str]:
    """Run @task"""

    fn_list = []

    for index, fn_config_tpl in enumerate(fn_config_list):
        hence_log("debug", "`run_tasks` :: %s", fn_config_tpl)

        if len(fn_config_tpl) > 2:

            raise ValueError(
                f"Only function and parameters are allowed in `{run_tasks.__name__}`"
            )

        fn_config = FuncConfig(sid=str(index), rid=run_id, *fn_config_tpl)
        hence_config.context_add(fn_config)

        fn_list.append(fn_config.task_key)

    if not fn_list:
        hence_log("error", "`fn_list` does not contain any `@task`.")
        raise TypeError("`fn_list` does not contain any `@task`.")

    _dag = setup_dag(fn_list)
    return execute_dag(_dag, SequentialProcessor(), FunctionTypeExecutor())


def run_group(group_id: str, task_params: list[dict]) -> Any:
    """Run groups"""

    groups = hence_config.context_get(CTX_GR_BASE)

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

    for index in range(1, len(vertices)):
        _dag.add_edge(vertices[index - 1], vertices[index])

    return _dag


def execute_dag(
    dag: DAG,
    ps: Union[SequentialProcessor, MultiThreadProcessor],
    ex: ExecutorContract,
) -> list:
    """Execute the dag"""

    if not isinstance(dag, DAG):
        raise TypeError(f"Not a DAG. type: {type(dag)}")

    return dag_run(dag, processor=ps, executor=ex)


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

    RES_KEY = "__works__"

    def __init__(self) -> None:
        """init FunctionTypeExecutor"""

        self._results = {}

    def param(self, vertex: Any) -> Any:
        """Selecting parameters"""

        return vertex

    def execute(self, task_key: str) -> Any:
        """Execute"""

        fn_cfg: FuncConfig = hence_config.context_get(CTX_FN_BASE, task_key)

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
            hence_config.context_add(fn_cfg)

        hence_log("debug", "`%s::%s` is executing.", t_title, task_key)

        return fn_cfg.function(**fn_cfg.parameters)

    def report_finish(self, vertices_result: list):
        """After execution finished"""

        fn_key, fn_result = vertices_result[0]

        fn_obj: FuncConfig = hence_config.context_get(CTX_FN_BASE, fn_key)
        fn_obj.result = fn_result

        hence_config.context_add(fn_obj)
