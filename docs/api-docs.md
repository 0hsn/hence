# API Docs

- [Hence Config](#hence-config)
  - [Enable logging](#enable-logging)
  - [Access task result after running tasks](#access-task-result-after-running-tasks)
  - [Access `FuncConfig` as a dict](#access-funcconfig-as-a-dict)
  - [Access `task_key` from `FuncConfig`](#access-task_key-from-funcconfig)
- [Task](#task)
  - [Define a task and run it](#define-a-task-and-run-it)
  - [Run tasks with no params](#run-tasks-with-no-params)
  - [Run tasks with params](#run-tasks-with-params)
  - [Run tasks with `run_id`](#run-tasks-with-run_id)

---

## Utils

_Added in `[v0.10.*]`_

Hence config is a multipurpose utility, such as access context data, logging, etc. A global hence configuration is already created on module loading.

Context holds all the internal data to be executed as well as results after execution.

### Enable logging

```python
from hence import hence_config as hc

...
# to enable logging to stderr
hc.enable_log = True
```

### Access task result after running tasks

It is possible to access internal state data in between task steps using `hence_config.task`.

Signature:

```http
HenceConfig.task(task_key: str)

Parameters:
    task_key: str, a unique task key.

Returns:
    - Resulting FuncConfig object for the function key given.
    - None when function not found.
```

For example

```python
from hence import hence_config as hc

task_keys = run_tasks(
    [
        (function_name, {})
    ]
)

# returns FuncConfig  from context
task_inf = hc.task(task_keys[0])

# Contains task result when task is executed or None
task_inf.result
```

### Access task object as a dict

```python
...

# returns FuncConfig  from context
task_inf = hence_config.task("function_name")
task_inf_d = task_inf.asdict()
```

### Access task key for a task

When you pass a `run_id` as a `run_tasks()` parameter, the function key gets updated. See [details here](#run-tasks-with-run_id). You can access that unique task execution identifier as follows.

```python
...
run_tasks(
    [
        (function_name, {})
    ]
)

# returns FuncConfig  from context
task_inf = hence_config.task("function_name")
task_inf.task_key # str[function_name.456]
```

#### Task key format

Task keys are generated dynamically after each successful `run_tasks` run.

```http
function_name.sequence_id[.run_id]

sections:
    function_name: str, name of the function that was executed.
    sequence_id: str, index number in the run_tasks.fn_config_list
    run_id: str, Optional (if passed to run_tasks), for unique run id. Slug: [a-zA-z0-9\-_]
```

`FuncConfig` object holds details of a function execution. For each function that is passed to `run_tasks` a `FuncConfig` object is created and saved in the context.

## Task

_Added in `[v0.9.*]`_

An alternative implementation for `@work` decorator, that is more cognitive friendly. To create a task based minimal workflow. Task represents smallest unit of work.

Task can be created using `@task()` decorator.

Here is [web scraper](../tests/samples/web_scraping_2.py) implemented.

**Parameters**

`title` String type. Represents the title of the task. Title can contain `{fn_key}`, `{fn_name}`, `{fn_run_id}` these values to distinguish from other similar group.

### Define a task and run it

```python
@task()
def fn_1(**kwargs):
  assert kwargs["var1"] == 1
  assert kwargs["val2"] == "sample string"

fn_1(var1=1, val2="sample string")
```

### Run tasks

`run_tasks()` receives a list of tuple, where 1st tuple item is a `@task` designated function handle, to be executed. 2nd item is a dictionary, where keys are the function parameter name, and values are value to be passed to the parameter.

Signature:

```http
def run_tasks(fn_config_list: list[tuple], run_id: str = "") -> list[str]:

Parameters:
    fn_config_list: List, containing FuncConfig data.
    run_id: str, for unique run id. Slug: [a-zA-z0-9\-_]

Returns:
    - Resulting list of FuncConfig task_key. This can be used to pull data from context.
```


#### Run tasks with no params


```python
@task(title="")
def fn_1(**kwargs):
    assert len(kwargs) == 0

@task(title="")
def fn_2(**kwargs):
  assert len(kwargs) == 0

run_tasks([
  (fn_1, {}),
  (fn_2, {}),
])
```

#### Run tasks with params

> NOTE: we are able to run same function with different parameters, while using `run_tasks(...)`. See below example.

```python
@task(title="")
def fn_1(**kwargs):
  assert kwargs["var1"] == "string"
  assert kwargs["val2"] == 23


# run all the tasks
# - with no params to pass to tasks
run_tasks([
  (fn_1, {var1: "string one", var2: 23}),
  (fn_1, {var1: "string two", var2: 32}),
])
```

#### Run tasks with `run_id`

We are able to pass a unique id to separately identify a bulk of operation.

It is _IMPORTANT_ to keep `run_id` _**unique**_.

```python
from hence import task, run_tasks, hence_config

@task(title="")
def fn_1(**kwargs):
  assert kwargs["var1"] == "string"
  assert kwargs["val2"] == 23

# a unique run id
run_id = "some-random-unique"

# run all the tasks
run_tasks([
  (fn_1, {var1: "string", var2: 23}),
  (fn_1, {var1: "string", var2: 23}),
  (fn_1, {var1: "string", var2: 23}),
], run_id)
```

### Access results

`run_tasks(..)` always returns a list of task keys for an executed session. These keys can can used with `hence_config.task(..)` to get the result.

```python
from hence import task, run_tasks, hence_config

@task(title="")
def fn_1(**kwargs):
  assert kwargs["var1"] == "string"
  assert kwargs["val2"] == 23

# a unique run id
run_id = "some-random-unique"

# run all the tasks
task_keys = run_tasks([
  (fn_1, {var1: "string", var2: 23}),
  (fn_1, {var1: "string", var2: 23}),
  (fn_1, {var1: "string", var2: 23}),
], run_id)

# access run results
for task_k in task_keys:
    
    # get the FuncConfig object from context
    fn_conf = hence_config.task(task_k)

    print(fn_conf.result)
```
