# Embedded Cantor
For applications that require an embedded storage solution, for example Agents that require local persistence for 
caching or buffering, the embedded implementation of Cantor can be used. Embedded Cantor uses 
[H2](https://www.h2database.com/) for the underlying storage engine.

To use Cantor embedded in your application, add [this dependency](https://search.maven.org/artifact/com.salesforce.cantor/cantor-h2) 
to the `pom.xml`:
```xml
<dependency>
    <groupId>com.salesforce.cantor</groupId>
    <artifactId>cantor-h2</artifactId>
    <version>${cantor-h2.version}</version>
</dependency>
```

Here is a sample application:
```java
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;

import java.io.IOException;

class Example {
    public static void main(String[] args) throws IOException {
        String namespace = "namespace";
        String key = "key";
        byte[] value = "value".getBytes();

        // create an instance of Cantor on H2 and store the database in the given path
        Cantor cantor = new CantorOnH2("/tmp/example/db");
        
        // create the namespace if not exists
        cantor.objects().create(namespace);
        // store an object
        cantor.objects().store(namespace, key, value);
        byte[] returned = cantor.objects().get(namespace, key);
        
        System.out.println(
                "stored: " + new String(value) + "\n" +
                "retrieved: " + new String(returned)
        );
    }
}
```
