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

## Hence Config

_Added in `[v0.9.*]`_

Hence config is a multipurpose utility, such as access context data, logging, etc. A global hence configuration is already created on module loading.

### Enable logging

```python
from hence import hence_config

...
# to enable logging to stderr
hence_config.enable_log = True
```

### Access task result after running tasks

It is possible to access internal state data in between task steps using `hence_config.task`.

Signature:

```http
HenceConfig.task(obj_key: str)

Parameters:
    obj_key: string, name of the function to access result for.

Returns:
    - Resulting FuncConfig object for the function key given.
    - None when function not found.
```

```python
from hence import hence_config

...

# returns FuncConfig  from context
task_inf = hence_config.task("function_name")
task_inf.result # contains task result when task is executed or None
```

`FuncConfig` object holds details of a function execution. For each function that is passed to `run_tasks` a `FuncConfig` object is created and saved in the context.

### Access `FuncConfig` as a dict

```python
...

# returns FuncConfig  from context
task_inf = hence_config.task("function_name")
task_inf.asdict()
```

### Access `task_key` from `FuncConfig`

When you pass a `run_id` as a `run_tasks()` parameter, the function key gets updated. See [details here](#run-tasks-with-run_id). You can access that unique task execution identifier as follows.

```python
...

run_tasks([
    (function_name, {}, "456")
])

# returns FuncConfig  from context
task_inf = hence_config.task("function_name")
task_inf.task_key # str[function_name.456]
```

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

### Run tasks with no params

```python
@task(title="")
def fn_1(**kwargs):
  ...

@task(title="")
def fn_2(**kwargs):
  ...

run_tasks([
  (fn_1, {}),
  (fn_2, {}),
])
```

### Run tasks with params

```python
@task(title="")
def fn_1(**kwargs):
  assert kwargs["var1"] == "string"
  assert kwargs["val2"] == 23


# run all the tasks
# - with no params to pass to tasks
run_tasks([
  (fn_1, {var1: "string", var2: 23}),
])
```

### Run tasks with `run_id`

In the following example we are running the same function, therefore, we are passing different `run_id` to preserve each task info using different key.

It is _IMPORTANT_ to keep `run_id` _**unique**_.

```python
from hence import task, run_tasks, hence_config

@task(title="")
def fn_1(**kwargs):
  assert kwargs["var1"] == "string"
  assert kwargs["val2"] == 23

# run all the tasks
run_tasks([
  (fn_1, {var1: "string", var2: 23}, "x1"), # x1, x2, x3 is run_id
  (fn_1, {var1: "string", var2: 23}, "x2"),
  (fn_1, {var1: "string", var2: 23}, "x3"),
])
```

To access the result for each run, you can access as follows. `fn_1.x1`, `fn_1.x2`, `fn_1.x3` are unique function task_key or function run identifier. 

Function run identifier are made of `functon_name.run_id`.

```python
run_tasks([
    ...
])

fn_1_result = hence_config.task("fn_1.x1").result
fn_2_result = hence_config.task("fn_1.x2").result
fn_3_result = hence_config.task("fn_1.x3").result
```
