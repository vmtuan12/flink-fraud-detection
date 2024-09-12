from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, KeyedBroadcastProcessFunction
from pyflink.common import WatermarkStrategy, Encoder, Row
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetResetStrategy
from pyflink.datastream.state import MapStateDescriptor
from dotenv import load_dotenv
import os