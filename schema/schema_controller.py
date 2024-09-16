from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.formats.avro import AvroRowSerializationSchema, AvroRowDeserializationSchema

TRANSACTION_TYPE_INFO = Types.ROW_NAMED(["id", "user_id", "receiver", "amount", "created_at", "card_type"], [Types.INT(), Types.INT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.STRING()])
RULE_TYPE_INFO = Types.ROW_NAMED(["type", "amount", "duration", "card_type"], [Types.STRING(), Types.FLOAT(), Types.INT(), Types.STRING()])
ALERT_TYPE_INFO = Types.ROW_NAMED(["user_id", "card_type", "window_type", "message", "alerted_at"], [Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()])

class SerializationType():
    JSON = 0
    AVRO = 1

class SchemaControl():

    @classmethod
    def get_transaction_deserialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowDeserializationSchema.builder().type_info(type_info=TRANSACTION_TYPE_INFO).build()
        else:
            return AvroRowDeserializationSchema(avro_schema_string=open("schema/avsc/transaction.avsc", "r").read())

    @classmethod    
    def get_rule_deserialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowDeserializationSchema.builder().type_info(type_info=RULE_TYPE_INFO).build()
        else:
            return AvroRowDeserializationSchema(avro_schema_string=open("schema/avsc/rule.avsc", "r").read())

    @classmethod    
    def get_alert_serialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowSerializationSchema.builder().type_info(type_info=ALERT_TYPE_INFO).build()
        else:
            return AvroRowSerializationSchema(avro_schema_string=open("schema/avsc/alert.avsc", "r").read())