## Definition

The `Objects` interface defines key/value pairs, where keys are unique strings and the values are arbitrary byte arrays.

An `Object` looks like this:

```json
{
	"key1": "QmFzZTY0IGVuY29kZWQ="
}
```

## HTTP API

To make HTTP calls in local testing environment, use this base URL: [http://localhost:8084](http://localhost:8084).

For convenience, you can use [Cantor Swagger UI](http://localhost:8084), which comes with your local cantor instance, to compose the full custom URL for your API calls. Full URL to each API endpoint's Swagger UI page is linked on each endpoint below. **Remember to spin up your local cantor HTTP server instance before you click on any Swagger UI link on this page.**

Most of the `Objects` API endpoints need required and/or optional URL parameters. Required URL parameters are shown as part of endpoint's path, while optional URL parameters, if existed, are given below. Only one endpoint (i.e. `PUT /api​/objects​/{namespace}​/{key}`) needs data parameters.

### [GET /api/objects](http://localhost:8084/#/Objects%20Resource/getNamespaces_1)

Get all objects namespaces.

**Sample Code:**

```bash
curl -X GET "http://localhost:8084/api/objects" -H "accept: application/json"
```

### [GET ​/api​/objects​/{namespace}​/{key}](http://localhost:8084/#/Objects%20Resource/getByKey)

Get an object's content by its key.

**Sample Code:**

This HTTP call returns the object data for key `iebdj1s` under namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/objects/dev/iebdj1s" -H "accept: application/json"
```

### [GET ​/api​/objects​/size​/{namespace}](http://localhost:8084/#/Objects%20Resource/size)

View size of a namespace.

**Sample Code:**

This HTTP call returns the size of the namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/objects/size/dev" -H "accept: application/json"
```

### [GET /api​/objects​/keys​/{namespace}](http://localhost:8084/#/Objects%20Resource/keys)

Get the keys of objects in a namespace.

**Optional URL Paramemter(s):**

- `start`: `integer`

	Start offset.

- `count`: `integer`

	Maximum number of key entries to return.

**Sample Code:**

This HTTP call returns the first 20 keys of the namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/objects/keys/dev?start=0&count=20" -H "accept: application/json"
```

### [PUT /api​/objects​/{namespace}](http://localhost:8084/#/Objects%20Resource/create_1)

Create a new object namespace.

**Sample Code:**

This HTTP call creates the namespace `dev`.

```bash
curl -X PUT "http://localhost:8084/api/objects/dev" -H "accept: */*"
```

### [PUT /api​/objects​/{namespace}​/{key}](http://localhost:8084/#/Objects%20Resource/store)

Add or overwrite an object in a namespace.

**Sample Code:**

This HTTP call adds or overwrite the object with key `key1` under namespace `dev` with the value `QmFzZTY0IGVuY29kZWQ=`.

```bash
curl -X PUT "http://localhost:8084/api/objects/dev/key1" -H "accept: */*" -H "Content-Type: text/plain" -d "[\"QmFzZTY0IGVuY29kZWQ=\"]"
```

### [DELETE ​/api​/objects​/{namespace}](http://localhost:8084/#/Objects%20Resource/drop_1)

Drop an object namespace.

**Sample Code:**

This HTTP call drops the namespace `dev`.

```bash
curl -X DELETE "http://localhost:8084/api/objects/dev" -H "accept: */*"
```

### [DELETE ​/api​/objects​/{namespace}​/{key}](http://localhost:8084/#/Objects%20Resource/deleteByKey)

Delete an object by its key.

**Sample Code:**

This HTTP call deletes the object with key `key1` under the namespace `dev`.

```bash
curl -X DELETE "http://localhost:8084/api/objects/dev/key1" -H "accept: application/json"
```

## Java gRPC API

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
