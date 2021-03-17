## Definition

Cantor `Events` are multi-dimensional time-series data points; where each data point has a *timestamp* (in milliseconds) along with some arbitrary key/value pairs as *metadata* (where values are strings), some arbitrary key/value pairs as *dimensions* (where values are doubles), and optionally a byte array *payload* attached to an event.

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

### HTTP API

To make HTTP calls in local testing environment, use this base URL: [http://localhost:8084](http://localhost:8084).

For convenience, you can use [Cantor Swagger UI](http://localhost:8084) linked on each of the API endpoints to compose the full custom URL for your API calls.

Most of the API calls contain both *required* and *optional* parameters. Required parameters are shown as part of endpoint's path, while optional parameters, if existed, are given below.

#### [GET /api/events](http://localhost:8084/#/Events%20Resource/getNamespaces_3)
Get all event namespaces.

**Sample Code:**
```bash
curl -X GET "http://localhost:8084/api/events" -H "accept: application/json"
```
#### [GET /api/events/{namespace}](http://localhost:8084/#/Events%20Resource/getEvents_1)
Get a list of events under a specific namespace.

**Optional Paramemter(s):**

- `start`: `integer`

    UTC format in milliseconds.

- `end`: `integer`

    UTC format in milliseconds.

- `metadata_query`: `string array`

    There are two kinds of metadata query you can use:

    - exact match, e.g. `["host=localhost"]` matches only the events whose metadata value for metadata key `host` is exactly `localhost`

    - regex match using `~` and `*`, e.g. `["host=~prod-*-example"]` matches only the events whose metadata value for metadata key `host` starts with `prod-` and ends with `-example`

- `dimensions_query`: `string array`

    You can also use `=`, `<`, `<=`, `>=` or `>` as part of dimensions query, e.g. `["cpu>=0.3"]` matches only the events whose dimension value for dimension key `cpu` has a value higher than or equal to `0.3`. 

- `include_payloads`: `boolean`

    Defaulted to `false`. Responses will include the payload of the events if set to `true`.

- `ascending`: `boolean`

    Defaulted to `true`. Events returned will be sorted in ascending order if set to `true` and vice versa.

- `limit`: `integer`

    Defaulted to `0`, which puts no limit on the number of events returned. If specified, this parameter limits the maximum number of events returned.

**Sample Code:**

This mock API call returns a list of events under namespace `test-namespace` between starting timestamp `10` and ending timestamp `20`, where alll values for the metadata key `host` starts with `na4-` (`["host=~na4-\*"]`) and alll values for the dimension key `cpu` is larger than or equal to 0.3 (`["cpu>=0.3"]`).

```bash
curl -X GET "http://localhost:8084/api/events/test-namespace?start=10&end=20&metadata_query=host%3D~na4-%2A&dimensions_query=cpu%3C%3D0.5&ascending=true" -H "accept: application/json"
```

#### [GET /api/events/{namespace}/metadata/{metadata}](http://localhost:8084/#/Events%20Resource/getMetadata_1)

Get all existing metadata values, given an event metadata key, under a specific event namespace.

**Optional Paramemter(s):**

- `start`: `integer`

    UTC format in milliseconds.

- `end`: `integer`

    UTC format in milliseconds.

- `metadata_query`: `string array`

    There are two kinds of metadata query you can use:

    - exact match, e.g. `["host=localhost"]` matches only the events whose metadata value for metadata key `host` is exactly `localhost`

    - regex match using `~` and `*`, e.g. `["host=~prod-*-example"]` matches only the events whose metadata value for metadata key `host` starts with `prod-` and ends with `-example`

- `dimensions_query`: `string array`

    You can also use `=`, `<`, `<=`, `>=` or `>` as part of dimensions query, e.g. `["cpu>=0.3"]` matches only the events whose dimension value for dimension key `cpu` has a value higher than or equal to `0.3`. 

**Sample Code:**

This mock API call returns all possible metadata values for the metadata key `mkey` under namespace `test-namespace`, between starting timestamp `100` and ending timestamp `200`, with the metadata query `["host=~\*-search"]` and dimensions query `["heap<8000"]`.

```bash
curl -X GET "http://localhost:8084/api/events/test-namespace/metadata/mkey?start=100&end=200&metadata_query=host%3D~%2A-search&dimensions_query=heap%3C8000" -H "accept: application/json"
```

### Java client

**Sample Code:**
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

### gRPC client

## Use Case

Any time-series data fits will as a use case for events. Keep a log of requests to your web-service, with query parameters and headers as metadata. Add response size, request latency,  and number of errors as dimensions. You could even store the response body as a payload. Metadata and dimension keys do not have to be defined beforehand, so new keys can crop up at runtime without needing to change any schema or config.
