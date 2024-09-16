from pyflink.datastream import RuntimeContext, KeyedBroadcastProcessFunction, BroadcastProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from typing import Any
from utils.rule import CardType, AggregationType
from utils.accumulator_creator import AccumulatorCreator
import time

class FraudProcessFunction(KeyedBroadcastProcessFunction):
    def __init__(self):
        transaction_type_info = Types.ROW_NAMED(["id", "user_id", "receiver", "amount", "created_at", "card_type"], [Types.INT(), Types.INT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.STRING()])
        rule_type_info = Types.ROW_NAMED(["type", "amount", "duration", "card_type"], [Types.STRING(), Types.FLOAT(), Types.INT(), Types.STRING()])

        self._window_state_desc = MapStateDescriptor(
            "window_transactions",
            Types.INT(),
            Types.LIST(transaction_type_info)
        )
        self._rule_state_desc = MapStateDescriptor(
            "rule",
            Types.STRING(),
            rule_type_info
        )
        self._window_state = None

    def open(self, runtime_context: RuntimeContext):
        self._window_state = runtime_context.get_map_state(self._window_state_desc)

    def process_broadcast_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.Context):
        ctx.get_broadcast_state(self._rule_state_desc).put(value["card_type"], value)

    # todo
    def process_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext):
        card_type = value["card_type"]
        current_timestamp = value["created_at"]

        rule = ctx.get_broadcast_state(self._rule_state_desc).get(card_type)
        if rule == None:
            return
        
        self._add_transaction_to_state(event_time=current_timestamp, row=value)
        
        window_duration = rule["duration"]
        window_type = rule["type"]
        rule_amount = rule["amount"]
        
        accumulator = AccumulatorCreator.get_accumulator(type=window_type)
        for event_time in self._window_state.keys():
            if self._transaction_time_is_valid(transaction_time=event_time, start_time=(current_timestamp - window_duration), current_time=current_timestamp):
                transaction_list = self._window_state.get(event_time)
                [accumulator.add(t["amount"]) for t in transaction_list if t["card_type"] == card_type]

        aggregated_result = accumulator.get_local_value()
        rule_violated = accumulator.check_rule(rule_amount)
        if rule_violated:
            yield Row(value["user_id"], card_type, window_type, f"Rule violated! {aggregated_result} is greater than limit {rule_amount}", int(time.time()))
        
        ctx.timer_service().register_event_time_timer(current_timestamp * 1000)

    # todo
    def on_timer(self, timestamp: int, ctx: KeyedBroadcastProcessFunction.OnTimerContext):
        window = self._window_state
        events = list(window.keys()).copy()

        card_type = ctx.get_current_key()[1]
        rule = ctx.get_broadcast_state(self._rule_state_desc).get(card_type)
        window_duration = rule["duration"] * 1000
        limit_timestamp = timestamp - window_duration

        for event_time in events:
            if (event_time * 1000) < limit_timestamp:
                self._window_state.remove(event_time)

        yield from []

    def _add_transaction_to_state(self, event_time: int, row):
        transaction_list = self._window_state.get(event_time)
        if transaction_list != None:
            transaction_list.append(row)
        else:
            transaction_list = [row]

        self._window_state.put(event_time, transaction_list)

    def _transaction_time_is_valid(self, transaction_time: int, start_time: int, current_time: int):
        return start_time <= transaction_time and transaction_time <= current_time