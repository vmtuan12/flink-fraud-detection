from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types, TypeInformation
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, KeyedBroadcastProcessFunction
from pyflink.common import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetResetStrategy
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.time import Duration
from dotenv import load_dotenv
import os
from functions.functions import FraudProcessFunction

load_dotenv()

kafka_host = os.getenv('KAFKA_HOST')

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(interval=2000)
env.set_python_executable("/home/mhtuan/anaconda3/envs/flink-env/bin/python")

transaction_type_info = Types.ROW_NAMED(["id", "user_id", "receiver", "amount", "created_at", "card_type"], [Types.INT(), Types.INT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.STRING()])
rule_type_info = Types.ROW_NAMED(["type", "amount", "duration", "card_type"], [Types.STRING(), Types.FLOAT(), Types.INT(), Types.STRING()])
transaction_deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=transaction_type_info).build()
rule_deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=rule_type_info).build()

transaction_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
        .set_topics("fraud.transaction") \
        .set_group_id("flink-2") \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.LATEST)) \
        .set_value_only_deserializer(transaction_deserialization_schema) \
        .build()

rule_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
        .set_topics("fraud.rule") \
        .set_group_id("flink-6") \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(rule_deserialization_schema) \
        .build()

# accept records late 10s
ds_transaction = env.from_source(transaction_source, WatermarkStrategy.for_monotonous_timestamps().with_idleness(Duration.of_seconds(10)), "Transaction Source")
ds_rule = env.from_source(rule_source, WatermarkStrategy.for_monotonous_timestamps().with_idleness(Duration.of_seconds(10)), "Rule Source")

key_transaction_ds = ds_transaction.key_by(lambda record: (record["user_id"], record["card_type"]), key_type=Types.TUPLE([Types.INT(), Types.STRING()]))

rule_state_desc = MapStateDescriptor(
    "rule",
    Types.STRING(),
    rule_type_info
)

rule_broadcast_stream = ds_rule.broadcast(rule_state_desc)

detection_ds = key_transaction_ds.connect(rule_broadcast_stream)\
                                .process(FraudProcessFunction())
detection_ds.print()

env.execute("streaming_fraud_detection")