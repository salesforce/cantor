## Definition

The `Objects` interface defines key/value pairs, where keys are unique strings and the values are arbitrary byte arrays.

An `Object` looks like this:

```json
{
	"obj1": "QmFzZTY0IGVuY29kZWQ="
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

This HTTP call adds or overwrite the object with key `obj1` under namespace `dev` with the value `QmFzZTY0IGVuY29kZWQ=`.

```bash
curl -X PUT "http://localhost:8084/api/objects/dev/obj1" -H "accept: */*" -H "Content-Type: text/plain" -d "[\"QmFzZTY0IGVuY29kZWQ=\"]"
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

This HTTP call deletes the object with key `obj1` under the namespace `dev`.

```bash
curl -X DELETE "http://localhost:8084/api/objects/dev/obj1" -H "accept: application/json"
```

## Java gRPC API

### **namespaces()**

Get all object namespaces.

**Method Signature(s):** 

- `Collection<String> namespaces() throws IOException`

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.objects().namespace());
    }
}
```

### **create()**

Create an object namespace.

**Method Signature(s):**

- `void create(String namespace) throws IOException`

**Sample Code:**

The following code creates an object namespace `dev`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.objects().create("dev");
    }
}
```

### **drop()**

Drop an object namespace.

**Method Signature(s):** 

- `void drop(String namespace) throws IOException`

**Sample Code:**

The following code drops an object namespace `dev`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;​

class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.objects().drop("dev");
    }
}
```

### **keys()**

Returns paginated list of key entries in a namespace; the returned list is not ordered.

**Argument(s):**

- `start`: Start offset.

- `count`: Maximum number of key entries to return; -1 for infinite entries.

**Method Signature(s):** 

- `Collection<String> keys(String namespace, int start, int count) throws IOException`

**Sample Code:**

The following code prints the first 20 object keys under the namespace `dev`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.objects().keys("dev", 0, 20));
    }
}
```

### **store()**

Add or overwrite an object in a namespace.

**Method Signature(s):**

For storing a single object:

- `void store(String namespace, String key, byte[] bytes) throws IOException`

For storing multiple objects:

- `void store(String namespace, Map<String, byte[]> batch) throws IOException`

**Sample Code:**

The following code adds or overwrite the object with key `obj1` under namespace `dev` with the byte array derived from string `object data`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        cantor.objects.store("dev", Collections.singletonMap("obj1", "object data".getBytes(StandardCharsets.UTF_8)));
    }
}
```

### **get()**

Get an object's content by its key.

**Method Signature(s):**

- `byte[] get(String namespace, String key) throws IOException` Returns bytes associated to the given key.

- `Map<String, byte[]> get(String namespace, Collection<String> keys) throws IOException` Returns batch of key/values for the list of key entries.

**Sample Code:**

The following code retrieve the content of object with key`obj1` under namespace `dev`.

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.objects.get("dev", "obj1"));
    }
}
```

### **delete()**

Delete object(s) by key(s).

**Method Signature(s):**

- `boolean delete(String namespace, String key) throws IOException` Delete the object; return true if object was found and removed successfully, false otherwise.

- `void delete(String namespace, Collection<String> keys) throws IOException` Delete batch of objects.

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.objects.delete("dev", "obj1"));
    }
}
```

### **size()**

Returns number of key/value pairs in the given namespace.

**Method Signature(s):**

- `int size(String namespace) throws IOException`

**Sample Code:**

```java
import com.salesforce.cantor.grpc.CantorOnGrpc;
import java.io.IOException;
​
class Scratch {
    public static void main(String[] args) throws IOException {
        CantorOnGrpc cantor = new CantorOnGrpc("localhost:7443");
        System.out.println(cantor.objects.size("dev"));
    }
}
```

## Use Case

Use cases for the objects are effectively endless, storing key/value pairs is extremely common. Since values are arbitrary byte arrays, any content can be stored and retrieved using objects. Combining objects and sets can provide a large amount of utility, without spending a lot of time writing storage code. For example, create a set for the script name “email_admins.sh” with entries for each version, then store the script versions in objects.
