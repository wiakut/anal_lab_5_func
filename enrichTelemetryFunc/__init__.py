import azure.functions as func
import json
import redis
import daft

app = func.FunctionApp()

@app.function_name(name="enrichTelemetryFunc")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="iotmessages",
    connection="IOTHUB_EVENTHUB_CONNECTION"
)
def enrichTelemetryFunc(azeventhub: func.EventHubEvent):
    raw = azeventhub.get_body().decode('utf-8')
    data = json.loads(raw)
    turbine_id = data.get("turbine_id")

    r = redis.Redis.from_url(
        "rediss://:UzgKdfsI7GHnYtcWH9ngyzxZHNUAxBQtXAzCaKxJoQ4=@iotlabredis.redis.cache.windows.net:6380",
        decode_responses=True)

    metadata_bytes = r.get(turbine_id)
    if metadata_bytes is None:
        return

    metadata = json.loads(metadata_bytes)
    enriched = {**data, "metadata": metadata}

    df = daft.from_pydict({
        **{k: [enriched[k]] for k in enriched if k != "metadata"},
        "metadata": [json.dumps(enriched["metadata"])]
    })

    df.write_deltalake(
        "abfss://enriched-data@iotlabstoragemkhamuliak.dfs.core.windows.net/data",
        mode="append"
    )

main = app