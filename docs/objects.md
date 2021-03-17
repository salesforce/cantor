## Definition

The `Objects` interface defines actions surrounding storing and retrieving key/value pairs. Keys are unique strings and the values are arbitrary byte arrays. This is consistent with other key/value storage solutions: there are methods to create and drop namespaces, as well as methods to persist and retrieve objects.

## Usage

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
## Use Case

Use cases for the objects are effectively endless, storing key/value pairs is extremely common. Since values are arbitrary byte arrays, any content can be stored and retrieved using objects. Combining objects and sets can provide a large amount of utility, without spending a lot of time writing storage code. For example, create a set for the script name “email_admins.sh” with entries for each version, then store the script versions in objects.
