# Events

The `Events` interface defines actions dealing with context-rich events, with attached blob payloads. The `Events` interface stores multidimensional time-series data points; where each data point has a timestamp along with an arbitrary list of metadata (key/value strings), a number of dimensions (keys mapped to numeric values), and a payload (arbitrary byte array). For example, an `Events` instance can look like:

```json
{
    "timestampMillis": 0,
    "metadata": {
        "additionalProp1": "string",
        "additionalProp2": "string",
        "additionalProp3": "string"
	},
    "dimensions": {
	    "additionalProp1": 0,
	    "additionalProp2": 0,
	    "additionalProp3": 0
	},
    "payload": "QmFzZTY0IGVuY29kZWQ="
}
```

Any time-series data fits will as a use case for events. Keep a log of requests to your web-service, with query parameters and headers as metadata. Add response size, request latency,  and number of errors as dimensions. You could even store the response body as a payload. Metadata and dimension keys do not have to be defined beforehand, so new keys can crop up at runtime without needing to change any schema or config.
