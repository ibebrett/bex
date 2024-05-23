import asyncio
import numpy as np
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from websockets.server import serve, WebSocketServer
from collections import defaultdict

from pydantic import BaseModel
from typing import (
    Iterable,
    Dict,
    Generator,
    TypeVar,
    TypedDict,
    Generic,
    Coroutine,
    List,
    ClassVar,
    Type,
)
import random

PortType = TypeVar("PortType")


class Port(Generic[PortType]):
    queues: List[asyncio.Queue]

    def __init__(self, queues: List[asyncio.Queue]):
        self.queues = queues

    async def put(self, value: PortType):
        for queue in self.queues:
            await queue.put(value)


class GlobalInstrumentationMessage(BaseModel):
    time: float


SignalType = TypeVar("SignalType")


@dataclass
class Signal(Generic[SignalType]):
    time: float
    value: SignalType

    def __init__(self, value: SignalType):
        self.value = value
        self.time = time.time()


class SignalInstrumentation:
    pass


InputSignalDict = Dict[str, Type[Signal]]
OutputSignalDict = Dict[str, Type[Signal]]


class SignalProcessingNode(ABC):
    inputs: ClassVar[Type[InputSignalDict]]
    outputs: ClassVar[Type[OutputSignalDict]]

    @abstractmethod
    async def process(
        self,
        instrumentation: SignalInstrumentation,
        in_queues: Dict[str, asyncio.Queue[Signal]],
        out_ports: Dict[str, Port[Signal]],
    ) -> None: ...

    def stop(self) -> None: ...


class RandomSourceInput(TypedDict):
    pass


class RandomSourceOutput(TypedDict):
    main: Port[Signal[float]]


class RandomSource(SignalProcessingNode):
    stopped: bool

    inputs = {}
    outputs = {"main": Signal[float]}

    outputs: ClassVar[Type[OutputSignalDict]]

    def __init__(self):
        self.stopped = False

    async def process(
        self,
        instrumentation: SignalInstrumentation,
        in_queues: RandomSourceInput,
        out_ports: RandomSourceOutput,
    ) -> Coroutine:
        out_queue = out_ports["main"]
        while not self.stopped:
            await asyncio.sleep(0.01)
            val = random.random()
            await out_queue.put(Signal(val))

    def stop(self) -> None:
        self.stopped = True


class FourrierNodeInput(TypedDict):
    main: asyncio.Queue[Signal[float]]


class FourrierNodeOutput(TypedDict):
    main: Port[Signal[np.ndarray]]


class FourrierNode(SignalProcessingNode):
    inputs = {"main": Signal[float]}
    outputs = {"main": Signal[float]}

    stopped: bool

    def __init__(self, buffer_length: 10):
        self.stopped = False

    async def process(
        self,
        instrumentation: SignalInstrumentation,
        in_queues: FourrierNodeInput,
        out_ports: FourrierNodeOutput,
    ) -> None:
        in_queue = in_queues["main"]
        out_port = out_ports["main"]
        while not self.stopped:
            edge = await in_queue.get()

            val = np.array(np.sin(edge.value))
            await out_port.put(Signal(val))

            in_queue.task_done()

    def stop(self) -> None:
        self.stopped = True


@dataclass
class NodeInputEntry:
    node_name: str
    output_name: str


NodeInputs = Dict[str, NodeInputEntry]


class Pipeline:
    instrumentation: SignalInstrumentation
    nodes: Dict[str, SignalProcessingNode]
    input_map: Dict[str, NodeInputs]

    def __init__(self):
        self.sources = {}
        self.nodes = {}
        self.input_map = {}
        self.instrumentation = SignalInstrumentation()

    def add_node(
        self, name: str, node: SignalProcessingNode, inputs: NodeInputs
    ) -> None:
        assert not name in self.nodes
        self.nodes[name] = node

        # Validate the types of the pipeline.
        # for input_name, entry in inputs.items():
        #    source_node = self.nodes[entry.node_name]

        #    #print(source_node.outputs[entry.output_name].output_type)
        #    #print(node.inputs[input_name].input_type)
        #    #assert issubclass(
        #    #    source_node.outputs[entry.output_name].output_type,
        #    #    node.inputs[input_name].input_type,
        #    #)

        self.input_map[name] = inputs

    def get_global_instrumentation(self) -> GlobalInstrumentationMessage:
        return GlobalInstrumentationMessage(time=time.time())

    async def run_async(self):
        # Each input has a queue
        input_queue_map = {
            node_name: {
                input_name: asyncio.Queue() for input_name in node.inputs.keys()
            }
            for node_name, node in self.nodes.items()
        }
        # need a map of output_node, output_field -> list of
        subscription_map = defaultdict(list)
        for node_name, queue_map in input_queue_map.items():
            for input_name, queue in queue_map.items():
                entry = self.input_map[node_name][input_name]
                subscription_map[entry.output_name].append(queue)

        # Each output has a port, and it needs to add each input as a subscription
        output_port_map = {
            node_name: {
                output_name: Port(subscription_map[output_name])
                for output_name in node.outputs.keys()
            }
            for node_name, node in self.nodes.items()
        }

        async with asyncio.TaskGroup() as tg:
            node_tasks = {}
            for node_name, node in self.nodes.items():
                node_tasks[node_name] = tg.create_task(
                    node.process(
                        self.instrumentation,
                        input_queue_map[node_name],
                        output_port_map[node_name],
                    )
                )

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
    pipeline.add_node("random", RandomSource(), {})
    pipeline.add_node(
        "fourrier",
        FourrierNode(buffer_length=10),
        {"main": NodeInputEntry(node_name="random", output_name="main")},
    )

    api = API(pipeline)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(pipeline.run_async())

        async with serve(api.serve, "localhost", 8765):
            await asyncio.Future()  # run forever


asyncio.run(run_pipeline())
