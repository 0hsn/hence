# Alternative Implementation

An alternative implementation that is more congnative friendly. Less things to learn.

## IDEA

### Run tasks with no params

```python
@task(title="")
def fn_1(**kwargs):
  ...

@task(title="", needs=[fn_1])
def fn_2(**kwargs):
  ...

# run all the tasks
# - with no params to pass to tasks
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

@task(title="", needs=[fn_1])
def fn_2(**kwargs):
  ...

# run all the tasks
# - with no params to pass to tasks
run_tasks([
  (fn_1, {var1: "string", var2: 23}),
  (fn_2, {}),
])
```

### Run tasks as a groups passing not params to tasks

```python
@task(title="")
def fn_1(**kwargs):
  ...

# `needs` means this task depends on given list of task to executed before
# all the task listed to be executed parallely
@task(title="", needs=[fn_1])
def fn_2(**kwargs):
  ...


# `tasks` means list of tasks in the group
# all the task listed to be executed sequencially
a_task_group = group("group_for_a_task", tasks=[fn_1, fn_2])

run_group(a_task_group, [])
```

### Run tasks as a groups  passing params to tasks

```python
a_task_group = group("group_for_a_task")

@a_task_group
@task(title="")
def fn_1(**kwargs):
  ...

@a_task_group
@task(title="", needs=[fn_1])
def fn_2(**kwargs):
  ...

run_group(a_task_group, [null, {"var1": 1, "var2": 2}])
```

### Run tasks as a dependent groups

```python
a_task_group = group("group_for_a_task")

# `needs` means this task depends on given list of groups to executed before
# all the task groups listed to be executed parallely
b_task_group = group("group_for_b_task", needs=[a_task_group, ])


@a_task_group
@task(title="")
def fn_1(**kwargs):
  ...

@a_task_group
@task(title="", needs=[fn_1])
def fn_2(**kwargs):
  ...

run_group(b_task_group, [])
```
