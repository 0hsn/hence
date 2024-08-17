# API Docs

- [Task](#task)
  - [Define a minimal task and run it](#define-a-minimal-task-and-run-it)
  - [Run tasks in pipeline](#run-tasks-in-pipeline)
  - [Access results after pipeline execution](#access-results-after-pipeline-execution)
  - [Access previous step result in between execution](#access-previous-step-result-in-between-execution)
- [Utils](#ut)
  - [Enable logging](#enable-logging)
  - [Get task result after pipeline execution](#get-task-result-after-pipeline-execution)
  - [Get intermediate task result inside pipeline execution](#get-intermediate-task-result-inside-pipeline-execution)

---

## Task

A decorator, to create a task based minimal workflow. Task represents smallest unit of work.

Task can be created using `@task()` decorator.

Here is [web scraper](../tests/samples/web_scraping_2.py) implemented.

#### Parameters

`title` String type. Represents the title of the task. Title can contain following:

  `fn_task_key` adds the task key, <br>
  `fn_name` name of the function, <br>
  `fn_run_id` the run context id, <br>
  `fn_seq_id` the sequence number for as specific run, <br>

Example:

```python
@task(title="Some task title")
def some_task(**kwargs): ...

@task(title=f"Some task title - {fn_run_id}")
def some_task(**kwargs): ...

@task(title=f"Some task title - {fn_run_id}-{fn_seq_id}")
def some_task(**kwargs): ...
```

#### Task definition

Any valid python function, that represent a small unit of work, can be a task. In the above example `some_task(**kwargs)` is a task function.

`**kwargs` is a required parameter in all task function.

### Define a minimal task and run it

```python
@task()
def fn_1(**kwargs):
  assert kwargs["var1"] == 1
  assert kwargs["val2"] == "sample string"

fn_1(var1=1, val2="sample string")
```

This can be similarly written as,

```python
@task()
def fn_1(var1, val2, **kwargs):
  assert var1 == 1
  assert val2 == "sample string"

fn_1(var1=1, val2="sample string")
```

### Run tasks in pipeline

You can run multiple tasks one after another forming a pipeline using `run_tasks()` function.

#### Signature

```python
def run_tasks(fn_config_list: list[tuple], run_id: str = "") -> list[str]
```

#### Parameters

```yaml
Args:
  fn_config_list: List, containing TaskConfig data.
  run_id: str, for unique run id. Slug: [a-zA-z0-9\-_]

Returns:
  Resulting list of TaskConfig task_key. This can be used to pull data from context.
```

#### Remarks

`run_tasks()` receives a list of tuple, where 1st tuple item is a `@task` designated function handle, to be executed. 2nd item is a dictionary, where keys are the function parameter name, and values are value to be passed to the parameter.

#### Run tasks with no params

```python
@task()
def fn_1(**kwargs):
    assert len(kwargs) == 0

@task()
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
@task()
def fn_1(**kwargs):
  assert isinstance(kwargs["var1"], "string")
  assert isinstance(kwargs["val2"], 23)

# run all the tasks
run_tasks([
  (fn_1, {var1: "string one", var2: 23}),
  (fn_1, {var1: "string two", var2: 32}),
])
```

#### Run tasks with `run_id`

We are able to pass a unique id to separately identify a bulk of operation.

It is _IMPORTANT_ to keep `run_id` _**unique**_.

```python
from hence import task, run_tasks

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

### Access results after pipeline execution

It is possible to access task result data after successful task run using `Utils.get_task` static method.

`run_tasks(..)` always returns a list of task keys for an executed session. These keys can can used with `Utils.get_task` to get the TaskConfig.

#### Signature

```http
Utils.get_task(task_key: str)

Parameters:
    task_key: str, a unique task key.

Returns:
    - Resulting TaskConfig object for the function key given.
    - None when function not found.
```

#### Example

For example

```python
from hence import Utils

task_keys = run_tasks(
    [
        (function_name, {})
    ]
)

# returns TaskConfig  from context
task_inf = Utils.get_task(task_keys[0])

# Contains task result when task is executed or None
task_inf.result
```

More details example may look like following.

```python
from hence import task, run_tasks, Utils

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
    
    # get the TaskConfig object from context
    fn_conf = Utils.get_task(task_k)

    print(fn_conf.result)
```

### Access previous step result in between execution

It is possible to access internal state data in between task steps using `Utils.get_step` static method.

#### Signature

```http
Utils.get_step(seq_id: int, run_id: str)

Parameters:
    seq_id: int, position in the pipeline starting from 0
    run_id: str, unique run id for this run context

Returns:
    - Resulting TaskConfig object for the function key given.
    - None when function not found.
```

#### Example

For example

```python
from hence import Utils

@task()
def function_name_1(**kwargs) -> int:
  return 100

@task()
def function_name_2(mul, **kwargs):
    run_id = ""

    if "_META_" in kwargs:
        run_id = kwargs["_META_"]["run_id"]

    if len(run_id) != 0:
    
        # get step 0 result
        fn_data = Utils.get_step(0, run_id)

        return fn_data.result * mul

# run all tasks
task_keys = run_tasks(
    [
        (function_name_1, {})
        (function_name_2, {"mul": 2}) # passing param
    ]
)

# returns TaskConfig  from context
task_inf = Utils.get_task(task_keys[1])

# Contains task result when task is executed or None
assert task_inf.result == 200
```

## Utils

Hence config is a multipurpose utility, such as access context data, logging, etc. A global hence configuration is already created on module loading.

Context holds all the internal data to be executed as well as results after execution.

### Enable logging

```python
from hence import Utils

...
# to enable logging to stderr
Utils.enable_logging(True)
```

### Get task result after pipeline execution

[See here](#access-results-after-pipeline-execution)

### Get intermediate task result inside pipeline execution

[See here](#access-previous-step-result-in-between-execution)
