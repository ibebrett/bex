import asyncio
import numpy as np
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from websockets.server import serve, WebSocketServer

from pydantic import BaseModel
from typing import Iterable, Dict, Generator, TypeVar, Generic, Coroutine
import random


class GlobalInstrumentationMessage(BaseModel):
    time: float


InputType = TypeVar("InputType")


@dataclass
class SignalProcessingInput(Generic[InputType]):
    time: float
    inputs: InputType


OutputType = TypeVar("OutputType")


class SignalProcessingOutput(Generic[OutputType]):
    output: OutputType
    time: float

    def __init__(self, output: OutputType):
        self.output = output
        self.time = time.time()


class SignalInstrumentation:
    pass


class SignalProcessingSource(ABC):
    @abstractmethod
    async def process(
        self, instrumentation: SignalInstrumentation, out_queue: asyncio.Queue[float]
    ) -> Coroutine: ...

    def stop(self) -> None: ...


@dataclass
class SignalProcessingInput(Generic[InputType]):
    time: float
    inputs: InputType


class RandomSource(SignalProcessingSource):
    stopped: bool

    def __init__(self):
        self.stopped = False

    async def process(
        self,
        instrumentation: SignalInstrumentation,
        out_queue: asyncio.Queue[SignalProcessingOutput[float]],
    ) -> Coroutine:

        while not self.stopped:
            await asyncio.sleep(0.01)
            val = random.random()
            await out_queue.put(SignalProcessingOutput(val))

    def stop(self) -> None:
        self.stopped = True


class SignalProcessingNode(ABC):
    @abstractmethod
    async def process(
        self,
        instrumentation: SignalInstrumentation,
        in_queue: asyncio.Queue[SignalProcessingInput],
        out_queue: asyncio.Queue[SignalProcessingOutput],
    ) -> None: ...


class FourrierNode(SignalProcessingNode):
    stopped: bool

    def __init__(self, buffer_length: 10):
        self.stopped = False

    async def process(
        self,
        instrumentation: SignalInstrumentation,
        in_queue: asyncio.Queue[SignalProcessingInput[float]],
        out_queue: asyncio.Queue[SignalProcessingOutput[np.array]],
    ) -> None:
        while not self.stopped:
            edge = await in_queue.get()

            val = np.sin(edge.time)
            print("producing value", val)
            await out_queue.put(SignalProcessingOutput(val))

            in_queue.task_done()

    def stop(self) -> None:
        self.stopped = True


class Pipeline:
    sources: Dict[str, SignalProcessingSource]
    instrumentation: SignalInstrumentation

    def __init__(self):
        self.sources = {}
        self.nodes = {}
        self.clock_period = 0.1
        self.instrumentation = SignalInstrumentation()

    def add_source(self, name: str, source: SignalProcessingSource) -> None:
        assert not name in self.sources
        self.sources[name] = source

    def add_node(self, name: str, node: SignalProcessingNode) -> None:
        assert not name in self.nodes
        self.nodes[name] = node

    def get_global_instrumentation(self) -> GlobalInstrumentationMessage:
        return GlobalInstrumentationMessage(time=time.time())

    async def run_async(self):
        source_output_queues = {
            source_name: asyncio.Queue() for source_name in self.sources.keys()
        }
        async with asyncio.TaskGroup() as tg:
            source_tasks = {}
            for source_name, source in self.sources.items():
                source_tasks[source_name] = tg.create_task(
                    source.process(
                        self.instrumentation, source_output_queues[source_name]
                    )
                )
                await source_output_queues[
                    source_name
                ].get()  # for now throw this away.

            await asyncio.Future()  # run forever


class API:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    async def serve(self, websocket: WebSocketServer):
        async def handle_incoming():
            # We wnat to handle messages from clients
            async for message in websocket:
                # Send an instrumentation message every 0.1 seconds
                ...

        async def handle_outgoing():
            while True:
                await websocket.send(
                    self.pipeline.get_global_instrumentation().model_dump_json()
                )

        async with asyncio.TaskGroup() as tg:
            tg.create_task(handle_incoming())
            tg.create_task(handle_outgoing())


async def run_pipeline():
    pipeline = Pipeline()
    pipeline.add_source("random", RandomSource())

    api = API(pipeline)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(pipeline.run_async())

        async with serve(api.serve, "localhost", 8765):
            await asyncio.Future()  # run forever


asyncio.run(run_pipeline())
