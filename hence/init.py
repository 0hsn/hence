"""Hence module"""

import typing
from pydantic import BaseModel, Field


class PipelineContext(BaseModel):
    """Holds pipeline internal data"""

    result: dict[str, typing.Any] = Field(default_factory=dict)
    parameters: dict[str, typing.Any] = Field(default_factory=dict)
    sequence: list[str] = Field(default_factory=list)


class Pipeline(BaseModel):
    """Base Pipeline utility class"""

    context: PipelineContext = Field(default_factory=PipelineContext)

    def add_task(self): ...

    def re_add_task(self): ...

    def run(self): ...

    def parameter(self, **kwargs) -> typing.Self:
        """Pass parameter to a task"""
        for key in kwargs.keys():
            if key not in self.context.sequence:
                raise KeyError("An task unique id not registered.")

            self.context.parameters[key] = kwargs[key]

        return self
