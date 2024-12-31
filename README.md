LDI UV project demo 

Created on 31/12/24

Init UV project:
`uv init <projectName>`

Create venv: `uv venv --python 3.9`
Activate venv: `source .venv/bin/activate`

Install packages: `uv pip install pyspark`

Deploy infra: `make create-kafka-cluster`

Run code in different terminals: 
`python kafka_producer`

`python kafka_consumer_delta`
