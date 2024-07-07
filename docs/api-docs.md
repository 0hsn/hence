# API Docs

- [Task](#task)
  - [Define a task and run it](#define-a-task-and-run-it)
  - [Run tasks with no params](#run-tasks-with-no-params)
  - [Run tasks with params](#run-tasks-with-params)
- [Work](#work)
  - [Defining a work](#defining-a-work)
  - [Work with `before` and `after` hook](#work-with-before-and-after-hook)
  - [Access returned values from `before` hook in _work_](#access-returned-values-from-before-hook-in-work)
  - [Calling a work](#calling-a-work)
- [WorkExecFrame](#workexecframe)
  - [Defining a WorkExecFrame](#defining-a-workexecframe)
  - [Passing parameter to a Work via WorkExecFrame](#passing-parameter-to-a-work-via-workexecframe)
  - [Execute a WorkExecFrame](#execute-a-workexecframe)
  - [Access a WorkExecFrame output](#access-a-workexecframe-output)
- [WorkGroup](#workgroup)
  - [Using a WorkGroup](#using-a-workgroup)
  - [Accessing previous step data in runtime](#accessing-previous-step-data-in-runtime)
- [WorkFlow](#workflow)

---

## Task

_Added in `[v0.9]`_

An alternative implementation for `@work` decorator, that is more cognitive friendly. To create a task based minimal workflow. Task represents smallest unit of work.

Task can be created using `@task()` decorator.

**Parameters**

`title` String type. Represents the title of the task.

### Define a task and run it

```python
@task(title="")
def fn_1(**kwargs):
  ...

fn_1(var1=1, val2="sample string")
```

### Run tasks with no params

```python
@task(title="")
def fn_1(**kwargs):
  ...

@task(title="", needs=[fn_1])
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
  ...

# run all the tasks
# - with no params to pass to tasks
run_tasks([
  (fn_1, {var1: "string", var2: 23}),
])
```

---

## Work

Work hold a small unit of achievable to do. It's really just a callable function that does something, with some magic attached.

### Defining a work

The core of this library is **_work_**, a small group of python instructions. you can make any function a **_work_** when the function implement `@work(..)` decorator or is a subclass to `AbstractWork` that implement `__work__(..)` abstract method. e.g.

```python
class Sum(AbstractWork):
    """A sample child Work"""

    def __work__(self, **kwargs) -> None:
        """
        implemented abstract implementation that does a small task.
        In this case print a string.
        """

        print("Sum")
```

this is same a writing with annotation. i.e.

```python
@work()
def sum(**kwargs) -> None:
    """
    implemented decorated function that does a small task.
    In this case print a string.
    """

    print("Sum")
```

> `**kwargs` is a required parameter for a work definition. Library with raise exception if it is not added as parameter.

> work can `return` a any value as the work function return. See here for [how to use returned values](#).

### Work with `before` and `after` hook

It is possible to execute a hook function before and after for work definition. This is useful for setup and teardown works before and after execution of a work.

For a class based work definition, before and after can be added such as:

```python
class Sum(AbstractWork):

    def __work__(self, **kwargs) -> None:
        print("Sum")

    def __before__(self) -> None:
        print("Executed before Sum")

    def __after__(self) -> None:
        print("Executed after Sum")
```

In case of decorator based function, i.e.

```python
def before_hook(self) -> None:
    print("Executed before Sum")

def after_hook(self) -> None:
    print("Executed after Sum")

@work(before=before_hook, after=after_hook)
def sum(**kwargs) -> None:
    print("Sum")
```

### Access returned values from `before` hook in _work_

When a `before` hook given the return result from it can be accessed as `kwargs['__before__']`.

```python
class Sum(AbstractWork):

    def __work__(self, **kwargs) -> None:
        assert f"{kwargs['__before__']}.Sum" == "Before.Sum"

    def __before__(self) -> str:
        return "Before"
```

In case of decorator based function, i.e.

```python
def before_hook(self) -> None:
    print("Executed before Sum")

@work(before=before_hook, after=after_hook)
def sum(**kwargs) -> None:
    assert f"{kwargs['__before__']}.Sum" == "Before.Sum"
```

### Calling a work

Any working is a fully working python function so it can be called as such.

```python
class Sum(AbstractWork):

    def __work__(self, **kwargs) -> None:
        assert f"{kwargs['__before__']}.Sum" == "Before.Sum"

    def __before__(self) -> str:
        return "Before"

sum = Sum()
sum()
```

In case of decorator based function, i.e.

```python
def before_hook(self) -> None:
    print("Executed before Sum")

@work(before=before_hook, after=after_hook)
def sum(**kwargs) -> None:
    assert f"{kwargs['__before__']}.Sum" == "Before.Sum"

sum()
```

## WorkExecFrame

WorkExecFrame is a reactor for a Work. WorkExecFrame holds a work, execute it, store what the work producted as output.

### Defining a WorkExecFrame

Only a children of `AbstractWork` or `@work(..` decorated function can be added to `WorkExecFrame.function`.

```python
class Sum(AbstractWork):

    def __work__(self, **kwargs) -> None:
        assert f"{kwargs['__before__']}.Sum" == "Before.Sum"

    def __before__(self) -> str:
        return "Before"

WorkExecFrame(function=Sum())
```

In case of decorator based function, i.e.

```python
def before_hook(self) -> None:
    print("Executed before Sum")

@work(before=before_hook, after=after_hook)
def sum(**kwargs) -> None:
    assert f"{kwargs['__before__']}.Sum" == "Before.Sum"

WorkExecFrame(function=sum)
```

### Passing parameter to a Work via WorkExecFrame

Work function parameters can be passed on runtime of execution via WorkExecFrame as following.

```python
class Sum(AbstractWork):

    def __work__(self, **kwargs) -> None:
        assert kwargs.get("animal") == "Cow"

WorkExecFrame(function=Sum(), function_params={"animal": "Cow"})
```

In case of decorator based function, i.e.

```python
@work()
def sum(**kwargs) -> None:
    assert kwargs.get("animal") == "Cow"

WorkExecFrame(function=sum, function_params={"animal": "Cow"})
```

### Execute a WorkExecFrame

A function thats attached to WorkExecFrame can be executed utilizing parameters added using `run()` member function.

```python
@work()
def fn(**kwargs) -> None:
    ...

wef = WorkExecFrame(function=fn, function_params={"val": 1})
wef.run()
```

It's also possible to pass more named parameters to WorkExecFrame on the time of `run(**kwargs)` call.

```python
@work()
def fn(**kwargs) -> None:
    ...

wef = WorkExecFrame(function=fn, function_params={"val1": 1})

wef.run({"val2": 2, "val3": ["a", "b", "c"]})
```

### Access a WorkExecFrame output

After successful execution of `WorkExecFrame.run()` the function response gets saved in `WorkExecFrame.function_out` member. It can be directly accessed.

```python
@work()
def fn(**kwargs) -> Optional[str]:
    return kwargs.get("val")

wef = WorkExecFrame(function=fn, function_params={"val": 1})
wef.run()

assert wef.function_out == 1
```

> Please remember the values DO NOT get saved in `WorkExecFrame.function_out` in safe way. Therefore marshalling and unmarhalling lies on the users hand. For example, in [tests/samples/web_scraping.py](../tests/samples/web_scraping.py) see how `fetch_content` and `get_the_title` does marshalling and unmarhalling. Suggestion and discussion is alway invited to improve this behavior.

## WorkGroup

_WorkGroup_ is to make a group out of a collection of _WorkExecFrame_. _WorkGroup_ is the basic building block of work orchestration. _WorkGroup_ is capable of executing each _WorkExecFrame_ that is added to it.

### Using a WorkGroup

```python
@work()
def implemented_work1(**kwargs):
    return implemented_work1.__name__

@work()
def implemented_work2(**kwargs):
    return implemented_work2.__name__

@work()
def implemented_work3(**kwargs):
    return implemented_work3.__name__

wl = []

wl.append(
    WorkExecFrame(
        function=implemented_work1,
        function_params={"as": 2, "of": "date0"},
    )
)
wl.append(
    WorkExecFrame(
        function=implemented_work2,
        function_params={"as": 2, "of": "date1"},
    )
)
wl.append(
    WorkExecFrame(
        function=implemented_work3,
        function_params={"as": 2, "of": "date2"},
    )
)

wg = WorkGroup(wl)

wg.setup_dag()
wg.execute_dag()
```

`WorkGroup.execute_dag()` returns a list that contains all the _WorkExecFrame_ with execution results, can be accessible with `WorkExecFrame.function_out`.

### Accessing previous step data in runtime

When executing a WorkGroup, it possible to access previous state results in the _Work_ inside _WorkExecFrame_. See below example.

```python
@work()
def implemented_work1(**kwargs):
    return 1

@work()
def implemented_work2(**kwargs):
    return kwargs.get("__works__")["one"] + 1

@work()
def implemented_work3(**kwargs):
    return kwargs.get("__works__")["two"] + 1

wl = []

wl.append(
    WorkExecFrame(
        id_="one"
        function=implemented_work1,
        function_params={"as": 2, "of": "date0"},
    )
)
wl.append(
    WorkExecFrame(
        id_="two"
        function=implemented_work2,
        function_params={"as": 2, "of": "date1"},
    )
)
wl.append(
    WorkExecFrame(
        function=implemented_work3,
        function_params={"as": 2, "of": "date2"},
    )
)

wg = WorkGroup(wl)

wg.setup_dag()
wg.execute_dag()
```

## WorkFlow

_WorkFlow_ is a collection of _WorkGroup_. _WorkFlow_ is a top-level flow building block. e.g.

```python
class ImplementedWork1(AbstractWork):

    def __work__(self, **kwargs):
        print(1)

class ImplementedWork2(AbstractWork):

    def __work__(self, **kwargs):
        print(2)

class ImplementedWork3(AbstractWork):

    def __work__(self, **kwargs):
        print(3)

class ImplementedWork4(AbstractWork):

    def __work__(self, **kwargs):
        print(4)

wl1 = []
wl1.append(WorkExecFrame(function=ImplementedWork1()))
wl1.append(WorkExecFrame(function=ImplementedWork2()))

wl2 = []
wl2.append(WorkExecFrame(function=ImplementedWork3()))
wl2.append(WorkExecFrame(function=ImplementedWork4()))

wf = Workflow([
    WorkGroup(wl1),
    WorkGroup(wl2),
])

wf.execute_dag()
```

In the end to summarize: _WorkFlow_ holds a list of _WorkGroup_ objects. _WorkGroup_ holds a list of _WorkExecFrame_. _WorkExecFrame_ holds a function, its parameters and output after execution.
