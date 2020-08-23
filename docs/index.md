# Cantor

Cantor is a data serving layer: it offers persistence for a number of basic data structures on top of a variety of 
storage solutions (e.g., MySQL and S3). Cantor exposes an HTTP and gRPC API, and includes thin client libraries for 
Java and Python.

Basic persistent data structures offered by Cantor are:
- [**Objects**](./objects.md): are key/value pairs, where *key* is a string and *value* is a byte array. Users can store, retrieve, 
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

- [**Events**](./events.md): are schema-less time-series events, where each event can have number of dimensions (string to number 
pairs) and metadata (string to string pairs) attached to it. An event can also contain an arbitrary byte array as the 
payload. Users can store, delete, and query events.

Here is an example of how to use *Events* in Java:
```java
Cantor cantor = ...  // initialize an instance of Cantor
String namespace = ...  // choose a namespace
```


