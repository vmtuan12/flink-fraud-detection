# Simple Flink fraud detection

This project implements a simple application to detect fraudulent transactions made by users based on dynamic rules.

Download Flink and install to a folder. Flink releases can be found [here](https://flink.apache.org/downloads/)

Install required python libraries
```
pip install -r requirements.txt
```

Create `.env` based on `.env.example`

Produce messages to topics first, then run the application by the command below
```
<path to flink folder>/bin/flink run -py main.py --target local
```

Technologies used:
- Flink DataStream (Flink 1.19.1)
- Kafka
- Avro