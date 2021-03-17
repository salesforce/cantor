## Definition

Cantor `Events` are multi-dimensional time-series data points; where each data point has a *timestamp* (in milliseconds) along with some arbitrary key/value pairs as *metadata* (where values are strings), some arbitrary key/value pairs as *dimensions* (where values are doubles), and optionally a byte array *payload* attached to an event.

An event looks like this:

```json
{
    "timestampMillis": 1616011054775,
    "metadata": {
        "metadataKey1": "a",
        "metadataKey2": "b",
        "metadataKey3": "c"
	},
    "dimensions": {
	    "dimensionsKey1": 0.1,
	    "dimensionsKey2": 0.2,
	    "dimensionsKey3": 0.3
	},
    "payload": "QmFzZTY0IGVuY29kZWQ="
}
```

## HTTP API

To make HTTP calls in local testing environment, use this base URL: [http://localhost:8084](http://localhost:8084).

For convenience, you can use [Cantor Swagger UI](http://localhost:8084), which comes with your local cantor instance, to compose the full custom URL for your API calls. Full URL to each API endpoint's Swagger UI page is linked on each endpoint below. Remember to spin up your local cantor instance before you click on any API endpoint link.

Most of the `Events` API endpoints need required and/or optional URL parameters. Required URL parameters are shown as part of endpoint's path, while optional URL parameters, if existed, are given below. Only one endpoint (i.e. `POST ​/api​/events​/{namespace}`) needs data parameters.

### [GET /api/events](http://localhost:8084/#/Events%20Resource/getNamespaces_3)

Get all event namespaces.

**Sample Code:**

```bash
curl -X GET "http://localhost:8084/api/events" -H "accept: application/json"
```

### [GET /api/events/{namespace}](http://localhost:8084/#/Events%20Resource/getEvents_1)

Get all events under a specific namespace.

**Optional URL Paramemter(s):**

- `start`: `integer`

    UNIX Time.

- `end`: `integer`

    UNIX Time.

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

This mock API call returns a list of events under namespace `test-namespace` between starting timestamp `1616011054000` and ending timestamp `1616011055000`, where alll values for the metadata key `host` starts with `na4-` (`["host=~na4-\*"]`) and alll values for the dimension key `cpu` is larger than or equal to 0.3 (`["cpu>=0.3"]`).

```bash
curl -X GET "http://localhost:8084/api/events/test-namespace?start=1616011054000&end=1616011055000&metadata_query=host%3D~na4-%2A&dimensions_query=cpu%3C%3D0.5&ascending=true" -H "accept: application/json"
```

### [GET /api/events/{namespace}/metadata/{metadata}](http://localhost:8084/#/Events%20Resource/getMetadata_1)

Get all existing metadata values, for an event metadata key, under a specific event namespace.

**Optional URL Paramemter(s):**

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

This mock API call returns all possible metadata values for the metadata key `os` under namespace `test-namespace`, between starting timestamp `1616011054000` and ending timestamp `1616011055000`, with the metadata query `["host=~\*-search"]` and dimensions query `["mem<0.8"]`.

```bash
curl -X GET "http://localhost:8084/api/events/test-namespace/metadata/os?start=1616011054000&end=1616011055000&metadata_query=host%3D~%2A-search&dimensions_query=mem%3C0.8" -H "accept: application/json"
```

### [POST ​/api​/events​/{namespace}](http://localhost:8084/#/Events%20Resource/storeMultipleEvents_1)

Add event(s) under an event namespace.

**Data Paramemter(s):**

Include events to be added, e.g.

```json
[
    {
        "timestampMillis": 1616011054774,
        "metadata": {
            "metadataKey1": "a"
        },
        "payload": "QmFzZTY0IGVuY29kZWQ="
    },
    {
        "timestampMillis": 1616011054775,
        "dimensions": {
            "dimensionsKey2": 0.5
        }
    },
    {
        "timestampMillis": 1616011054776,
        "metadata": {
            "metadataKey3": "a"
        },
        "dimensions": {
            "dimensionsKey1": 0.1,
            "dimensionsKey3": 0.18
        }
    }
]
```

**Sample Code:**

This mock API call stores an event (schema defined under Definition on this page) under the event namespace `test-namespace`.

```bash
curl -X POST "http://localhost:8084/api/events/test-namespace" -H "accept: */*" -H "Content-Type: application/json" -d "[{\"timestampMillis\":1616011054774,\"metadata\":{\"metadataKey1\":\"a\"},\"payload\":\"QmFzZTY0IGVuY29kZWQ=\"},{\"timestampMillis\":1616011054775,\"dimensions\":{\"dimensionsKey2\":0.5}},{\"timestampMillis\":1616011054776,\"metadata\":{\"metadataKey3\":\"a\"},\"dimensions\":{\"dimensionsKey1\":0.1,\"dimensionsKey3\":0.18}}]"
```

### [PUT /api​/events​/{namespace}](http://localhost:8084/#/Events%20Resource/createNamespace_2)

Create an event namespace.

**Sample Code:**

This mock API call adds the event namespace `test-namespace`.

```bash
curl -X PUT "http://localhost:8084/api/events/test-namespace" -H "accept: */*"
```

### [DELETE ​/api​/events​/{namespace}](http://localhost:8084/#/Events%20Resource/dropNamespace_2)

Drop an event namespace.

**Sample Code:**

This mock API call drops the event namespace `test-namespace`.

```bash
curl -X DELETE "http://localhost:8084/api/events/test-namespace" -H "accept: */*"
```

### [DELETE /api​/events​/expire​/{namespace}​/{endTimestampMillis}](http://localhost:8084/#/Events%20Resource/expire_1)

Expire old events under a specific event namespace.

**Sample Code:**

This mock API call sets all events under the event namespace `test-namespace` to expire after UNIX time `1616020000`, which is March 17, 2021, at 8:43pm in UTC.

```bash
curl -X DELETE "http://localhost:8084/api/events/expire/test-namespace/1616020000" -H "accept: */*"
```

## Java gRPC API

### [namespaces()]((https://github.com/salesforce/cantor/blob/master/cantor-grpc-client/src/main/java/com/salesforce/cantor/grpc/EventsOnGrpc.java#L27-L33))

Get all event namespaces.

**Method Signature:** `Collection<String> namespaces() throws IOException`

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.events().namespace();
    }
}
```

### [create()](https://github.com/salesforce/cantor/blob/master/cantor-grpc-client/src/main/java/com/salesforce/cantor/grpc/EventsOnGrpc.java#L35-L45)

Create an event namespace.

**Method Signature:** `void create(final String namespace) throws IOException`

**Sample Code:**

This following code creates an event namespace `dev-namespace`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.events().create("dev-namespace");
    }
}
```

### [store()](https://github.com/salesforce/cantor/blob/master/cantor-grpc-client/src/main/java/com/salesforce/cantor/grpc/EventsOnGrpc.java#L59-L80)

Add event(s) under an event namespace.

**Method Signature:** `void store(final String namespace, final Collection<Event> batch) throws IOException`

**Sample Code:**

- TODO: add and explain the various store() methods

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        // remember to create the event namespace first
        cantor.events().store("dev-namespace", System.currentTimeMillis(), Collections.singletonMap("test-meta", "testing"), null, "Hello!".getBytes(StandardCharsets.UTF_8));
    }
}
```

### [get()](https://github.com/salesforce/cantor/blob/master/cantor-grpc-client/src/main/java/com/salesforce/cantor/grpc/EventsOnGrpc.java#L82-L119)

Get all events under a specific namespace.

**Method Signature:**
```java
List<Event> get(final String namespace,
                final long startTimestampMillis,
                final long endTimestampMillis,
                final Map<String, String> metadataQuery,
                final Map<String, String> dimensionsQuery,
                final boolean includePayloads,
                final boolean ascending,
                final int limit) throws IOException
```

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
​import java.io.IOException;

class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.events().get("dev-namespace", System.currentTimeMillis() - 60000, System.currentTimeMillis(), false));
    }
}
```

### [drop()](https://github.com/salesforce/cantor/blob/master/cantor-grpc-client/src/main/java/com/salesforce/cantor/grpc/EventsOnGrpc.java#L47-L57)

Drop an event namespace.

**Method Signature:** `void drop(final String namespace) throws IOException`

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;​

class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.events().drop("dev-namespace");
    }
}
```
### Code Snippet

This is a code snippet that sums up all methods mentioned above:

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
​
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.events().create("dev-namespace");
        cantor.events().store("dev-namespace", System.currentTimeMillis(), Collections.singletonMap("test-meta", "testing"), null, "Hello!".getBytes(StandardCharsets.UTF_8));
        System.out.println(cantor.events().get("dev-namespace", System.currentTimeMillis() - 60000, System.currentTimeMillis(), false));
        cantor.events().drop("dev-namespace");
    }
}
```

## Use Case

Any time-series data fits will as a use case for events. Keep a log of requests to your web-service, with query parameters and headers as metadata. Add response size, request latency,  and number of errors as dimensions. You could even store the response body as a payload. Metadata and dimension keys do not have to be defined beforehand, so new keys can crop up at runtime without needing to change any schema or config.
