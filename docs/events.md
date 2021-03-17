## Definition

Cantor `Events` are multi-dimensional time-series data points; where each data point has a *timestamp* (in milli-seconds) along with some arbitrary key/value pairs as *metadata* (where values are strings), some arbitrary key/value pairs as *dimensions* (where values are doubles), and optionally a byte array *payload* attached to an event.

An event looks roughly like this:
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

## Usage

Here is an example of how to use *Events* in Java:
```java
Cantor cantor = ...  // initialize an instance of Cantor
String namespace = ...  // choose a namespace
long timestamp = ...  // timestamp of the event
Map<String, Double> dimensions = ...  // map of string to double for dimensions
dimensions.put("zero", 0.0)  // add values
Map<String, String> metadata = ...  // map of string to string for metadata
metadata.put("key", "value")  // add values
byte[] payload = ...  // byte array as the payload of the event

cantor.events().create(namespace)  // create the namespace
cantor.events().store(namespace, timestamp, metadata, dimensions, payload)
// ...
Map<String, String> dimensionsQuery = ...  // dimension query object
dimensionsQuery.put("zero", ">=0.0")  // query for events where the value for dimension "zero" is greater than or equal to 0
cantor.events().get(namespace, timestamp - 1, timestamp + 1, dimensionsQuery, null)
```

## Use Case

Any time-series data fits will as a use case for events. Keep a log of requests to your web-service, with query parameters and headers as metadata. Add response size, request latency,  and number of errors as dimensions. You could even store the response body as a payload. Metadata and dimension keys do not have to be defined beforehand, so new keys can crop up at runtime without needing to change any schema or config.
