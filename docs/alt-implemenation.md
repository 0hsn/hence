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

---
```python
pipeline = Pipeline()

@pipeline.add_task(unique_id: str, pass_ctx: bool)

@pipeline.add_task(pass_ctx=true)
def task_1(ctx, param1, param2)

@pipeline.add_task(pass_ctx=true)
def task_2(ctx, param1, param2)
  ctx.result["task_1"]
  ctx.parameters["task_1"]
  ctx.sequence

pipeline.readd_task("task_3", task_1, pass_ctx=true)

@pipeline.add_task()
def task_4(param1, param2)
# for this case unique_id=task_4

pipeline.run()

pipeline
  .parameter(task_1={})
  .parameter(task_2=[])
  .parameter(task_3=22)
  .run()

PipelineOut()

p_out = pipeline.run()

p_out["task_1"]
```