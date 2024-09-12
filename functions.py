from pyflink.datastream import RuntimeContext, KeyedBroadcastProcessFunction, BroadcastProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from typing import Any
from rule import CardType, AggregationType

class FraudProcessFunction(KeyedBroadcastProcessFunction):
    def __init__(self):
        transaction_type_info = Types.ROW_NAMED(["id", "user_id", "receiver", "amount", "created_at", "card_type"], [Types.INT(), Types.INT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.STRING()])
        rule_type_info = Types.ROW_NAMED(["type", "amount", "duration", "card_type"], [Types.STRING(), Types.FLOAT(), Types.INT(), Types.STRING()])

        self._window_state_desc = MapStateDescriptor(
            "window_transactions",
            Types.INT(),
            transaction_type_info
        )
        self._rule_state_desc = MapStateDescriptor(
            "rule",
            Types.STRING(),
            rule_type_info
        )
        self._window_state = None

    def open(self, runtime_context: RuntimeContext):
        self._window_state = runtime_context.get_state(self._window_state_desc)

    def process_broadcast_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.Context):
        ctx.get_broadcast_state(self._rule_state_desc).put(value["card_type"], value)

    # todo
    def process_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext):
        pass

    # todo
    def on_timer(self, timestamp: int, ctx: KeyedBroadcastProcessFunction.OnTimerContext):
        pass