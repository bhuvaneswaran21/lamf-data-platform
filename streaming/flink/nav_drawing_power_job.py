
import json
import os

import redis
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import MapStateDescriptor

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")


SCHEME_LTV = {}  


class MarginCallDetector(KeyedProcessFunction):

    def open(self, ctx: RuntimeContext):
        self.dp = ctx.get_map_state(MapStateDescriptor("dp", Types.STRING(), Types.DOUBLE()))
        self.r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    def process_element(self, value, ctx):
        customer_id, scheme_code, dp_contrib, outstanding = value
        self.dp.put(scheme_code, dp_contrib)
        total_dp = sum(self.dp.values())

        self.r.hset(f"dp:{customer_id}", mapping={
            "drawing_power": round(total_dp, 2),
            "outstanding": round(outstanding, 2),
            "available": round(total_dp - outstanding, 2),
        })

        if outstanding > total_dp:
            shortfall = round(outstanding - total_dp, 2)
            self.r.hset(f"margincall:{customer_id}", mapping={
                "shortfall": shortfall, "status": "OPEN"})
            yield json.dumps({
                "event": "margin.call.raised", "customer_id": customer_id,
                "outstanding": outstanding, "drawing_power": round(total_dp, 2),
                "shortfall": shortfall,
            })
        else:
            self.r.delete(f"margincall:{customer_id}")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.add_jars()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("nav.updates")
        .set_group_id("flink-nav-dp")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            __import__("pyflink.common.serialization", fromlist=["SimpleStringSchema"]).SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "nav-source")


    enriched = ds.map(enrich, output_type=Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()]))

    alerts = enriched.key_by(lambda t: t[0]).process(MarginCallDetector())
    alerts.print()  

    env.execute("nav-drawing-power-margin-call")


def enrich(raw: str):
   
    msg = json.loads(raw)
    nav = float(msg["nav"])
    ltv = SCHEME_LTV.get(msg["scheme_code"], 50.0)
    dp_contrib = 100.0 * nav * ltv / 100.0
    return (f"cust-{msg['scheme_code']}", msg["scheme_code"], dp_contrib, dp_contrib * 0.95)


if __name__ == "__main__":
    main()
