# Hence - A minimal python workflow engine

## Introduction

Welcome to _Hence_, a powerful framework designed to streamline your workflow orchestration process. 

Whether you're involved in web scraping, data loading, fetching, or any other repetitive task, _Hence_ offers a comprehensive solution to break down these tasks into manageable units of work.

By orchestrating these units sequentially, _Hence_ empowers you to focus on the big picture without the hassle of manually ensuring the success of each step.

## Features

- **Task Breakdown** – Hence breaks complex tasks into smaller, manageable units for better organization and execution.
- **Workflow Orchestration** – Automate workflows with Hence to ensure smooth, sequential execution without manual effort.
- **Error Handling** – Hence manages errors gracefully, preventing workflow interruptions and ensuring seamless execution.
- **Scalability** – Whether small or large-scale, Hence adapts effortlessly to your needs for optimal performance.

## Use-cases

- **Web Scraping** – Hence automates web scraping by breaking tasks into fetching, extracting, and storing data.
- **Data Loading/Fetching** – Hence streamlines fetching from APIs and loading data into databases effortlessly.
- **Repetitive Tasks** – Automate reports, file processing, and data transformations with Hence to save time and effort.

## Setup / Installation

### Use as library

```shell
pip install -U git+https://github.com/0hsn/hence.git@main
```

### Development setup

#### Prerequisite

- [Poetry](https://python-poetry.org/docs/#installation)

#### Local installation steps

- Firstly, clone the repository
- Setup with development tools

  ```shell
  pipenv install --dev
  ```

### Testing

```shell
poetry run py.test -s
```

### Samples

```shell
poetry run python -m samples.web_scraping
```

## API

- [Pipeline](#pipeline)

  - [Pipeline.add_task](#pipelineadd_task)
  - [Pipeline.re_add_task](#pipelinere_add_task)
  - [Pipeline.parameter](#pipelineparameter)
  - [Pipeline.run](#pipelinerun)

- [PipelineContext](#pipelinecontext)

### Pipeline

#### Pipeline.add_task

Add a task to pipeline using decorator. This decorator is useful, when you want to define a function and make it pipeline task at the same time.

##### Signature

```python
def add_task(uid: typing.Optional[str] = None, pass_ctx: bool = False) -> typing.Any
```

##### Parameters

`uid: str | None` Optional. Default: `None`. A unique name for a task function in a pipeline. If same id passed, should replace older assignment.

`pass_ctx: bool` Optional. Default: `False`. Pass [PipelineContext](#pipelinecontext) as 1st parameter to task function. If true, the 1st parameter to the function

##### Example

```python
@pipeline.add_task(pass_ctx=True)
def function_1(ctx: PipelineContext, a: str):
    return a
```

#### Pipeline.re_add_task

Add a task to pipeline. This function is useful, when you want to define a function early and make it pipeline task later.

##### Signature

```python
def re_add_task(function: typing.Callable, uid: typing.Optional[str] = None, pass_ctx: bool = False) -> None
```

##### Parameters

`function: typing.Callable` Required. A function to act as a pipeline task.

`uid: str | None` Optional. Default: `None`. A unique name for a task function in a pipeline. If same id passed, should replace older assignment.

`pass_ctx: bool` Optional. Default: `False`. Pass [PipelineContext](#pipelinecontext) as 1st parameter to task function. If true, the 1st parameter to the function

##### Example

```python
def function_1(ctx: PipelineContext, a: str):
    return a

pipeline.re_add_task(function_1, pass_ctx=True)
```

#### Pipeline.parameter

Add parameters before [Pipeline.run](#pipelinerun). This function passes parameters when running the task.

##### Signature

```python
def parameter(self, **kwargs) -> typing.Self
```

##### Parameters

pass the function name or registered uid for the function as parameter.

##### Example

```python
def function_1(ctx: PipelineContext, a: str):
    return a

def function_2(ctx: PipelineContext, a: str):
    return a

pipeline.re_add_task(function_1, pass_ctx=True)
pipeline.re_add_task(function_2, uid="r_func")

pipeline
    .parameter(function_1={"a": "Some string"})
    .parameter(r_func={"a": "Some string"})
```

#### Pipeline.run

Run the pipeline.

##### Signature

```python
def run(self, is_parallel: bool = False) -> dict[str, typing.Any]:
```
##### Parameters

`is_parallel: bool` Optional. To run added tasks in parallel.

##### Example

```python
def function_1(ctx: PipelineContext, a: str):
    return a

def function_2(ctx: PipelineContext, a: str):
    return a

pipeline.re_add_task(function_1, pass_ctx=True)
pipeline.re_add_task(function_2, uid="r_func")

output = pipeline.run()

# or in parallel, since these tasks are not dependent
output = pipeline.run(True)
```

This function outputs a dictionary containing all function returns, by function name or uid (if used).

### PipelineContext

PipelineContext is a class that holds all the operation data for a certain [Pipeline](#pipeline).

- PipelineContext is passed when `.add_task(pass_ctx=True, ..` or `.re_add_task(.., pass_ctx=True, ..`.
- remember to add a variable as 1st parameter to function when `pass_ctx` is `True`.

##### Members

`result: dict[str, typing.Any]`. A dictionary containing returns from the executed functions in a certain pipeline.

`parameters: dict[str, dict[str, typing.Any]]` A dictionary containing all the parameters passed using [Pipeline.parameter](#pipelineparameter).

`sequence: list[str]` A list containing all the functions added as task to a certain pipeline.

`functions: dict[str, typing.Callable]` A dictionary containing all the functions added as task via [Pipeline.add_task](#pipelineadd_task) and [Pipeline.re_add_task](#pipelinere_add_task).

## Contributions

- Read [CONTRIBUTING](./CONTRIBUTING) document before you contribute.
- [Create issues](https://github.com/0hsn/hence/issues) for any questions or request
---

Licensed under [AGPL-3.0](./LICENSE)
