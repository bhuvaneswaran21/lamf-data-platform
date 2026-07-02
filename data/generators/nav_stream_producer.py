import argparse
import csv
import json
import time

from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="nav.updates")
    ap.add_argument("--input", default="../generated/nav_history.csv")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="ticks per second (throttle); 0 = no throttle")
    ap.add_argument("--loop", action="store_true", help="replay continuously")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    def emit_once():
        with open(args.input, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                msg = {
                    "scheme_code": row["scheme_code"],
                    "isin": row["isin"],
                    "nav": float(row["nav"]),
                    "nav_date": row["nav_date"],
                    "published_at": row["published_at"],
                }
                producer.send(args.topic, key=row["scheme_code"], value=msg)
                if args.speed:
                    time.sleep(1.0 / args.speed)
                if i % 1000 == 0:
                    producer.flush()
                    print(f"sent {i} ticks ...")
        producer.flush()

    emit_once()
    while args.loop:
        emit_once()
    print("done")


if __name__ == "__main__":
    main()
