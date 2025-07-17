This is pulled from `source-stripe` in airbyte

```json
{"type":"LOG","log":{"level":"INFO","message":"Starting syncing"}}
{"type":"LOG","log":{"level":"INFO","message":"Marking stream refunds as STARTED"}}
{"type":"LOG","log":{"level":"INFO","message":"Syncing stream: refunds "}}
{"type":"TRACE","trace":{"type":"STREAM_STATUS","emitted_at":1752778740025.7312,"stream_status":{"stream_descriptor":{"name":"refunds"},"status":"STARTED"}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1485302400"}},"sourceStats":{"recordCount":0.0}}}
{"type":"LOG","log":{"level":"INFO","message":"Marking stream refunds as RUNNING"}}
{"type":"TRACE","trace":{"type":"STREAM_STATUS","emitted_at":1752778741137.509,"stream_status":{"stream_descriptor":{"name":"refunds"},"status":"RUNNING"}}}
{"type":"RECORD","record":{"stream":"refunds","data":{"id":"re_3QAk24Ie3QHGADw921xJHKWn","object":"refund","amount":600,"balance_transaction":"txn_3QAk24Ie3QHGADw92zu9aqfS","charge":"ch_3QAk24Ie3QHGADw92yafs5Py","created":1729183834,"currency":"usd","destination_details":{"card":{"reference":"5994022553786635","reference_status":"available","reference_type":"acquirer_reference_number","type":"refund"},"type":"card"},"metadata":{},"payment_intent":"pi_3QAk24Ie3QHGADw92zRXNc9e","reason":"fraudulent","status":"succeeded","updated":1729183834},"emitted_at":1752778741137}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1729183834"}},"sourceStats":{"recordCount":1.0}}}
{"type":"RECORD","record":{"stream":"refunds","data":{"id":"pyr_1QlEWJIe3QHGADw9bd703YiM","object":"refund","amount":900,"balance_transaction":"txn_1QlEWJIe3QHGADw9YygiH1cR","charge":"py_3QlEVIIe3QHGADw91DYvYlOo","created":1737831351,"currency":"usd","destination_details":{"cashapp":{},"type":"cashapp"},"metadata":{},"payment_intent":"pi_3QlEVIIe3QHGADw912ZHa8OU","reason":"requested_by_customer","status":"succeeded","updated":1737831351},"emitted_at":1752778741353}}
{"type":"LOG","log":{"level":"INFO","message":"Read 2 records from refunds stream"}}
{"type":"LOG","log":{"level":"INFO","message":"Marking stream refunds as STOPPED"}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1737831351"}},"sourceStats":{"recordCount":1.0}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"updated":"1737831351"}},"sourceStats":{"recordCount":0.0}}}
{"type":"LOG","log":{"level":"INFO","message":"Finished syncing refunds"}}
{"type":"TRACE","trace":{"type":"STREAM_STATUS","emitted_at":1752778741354.892,"stream_status":{"stream_descriptor":{"name":"refunds"},"status":"COMPLETE"}}}
{"type":"LOG","log":{"level":"INFO","message":"Finished syncing"}}
```

updated this repo to match
