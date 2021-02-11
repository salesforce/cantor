Cantor is a data serving layer: it offers persistence for a number of basic data structures on top of a variety of 
storage solutions (e.g., MySQL and S3). Cantor exposes an HTTP and gRPC API, and includes thin client libraries for 
Java and Python.

Basic persistent data structures offered by Cantor are:

- [**Objects**](./objects.md): key/value pairs, where *key* is a string and *value* is a byte array. Users can store, retrieve, 
or delete an object, as well as retrieving list of object keys.

Here is an example of how to use *Objects* in Java:

```java
Cantor cantor = ...  // initialize an instance of Cantor
String namespace = ...  // choose a namespace
String key = ...  // key to lookup value later on
byte[] value = ...  // the byte array to be stored

cantor.objects().create(namespace)  // create the namespace 
cantor.objects().store(namespace, key, value)  // store the key/value pair in the namespace
// ...
byte[] retured = cantor.objects().get(namespace, key)  // retrieve the object 
cantor.objects().delete(namespace, key)  // remove the object 
```

- [**Events**](./events.md): time-series events, where each event can have a number of dimensions (string to number 
pairs) and metadata (string to string pairs) attached to it. An event can also contain an arbitrary byte array as the 
payload. Users can store, delete, and query events.

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

