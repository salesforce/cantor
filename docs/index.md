Cantor is a data serving layer: it offers persistence for a number of basic data structures on top of a variety of 
storage solutions (e.g., MySQL and S3). Cantor exposes an HTTP and gRPC API, and includes thin client libraries for 
Java and Python.

Cantor allows users to persist and query data stored in one of the following forms:

**Fat Events** - multi-dimensional time-series data points; where each data point has a timestamp along with an arbitrary list of metadata (key/value strings), a number of dimensions (double values), and a payload (arbitrary byte array).

**Key/value pairs (Objects)** - the key is a unique string and the value is an arbitrary byte array. This is consistent with other key/value storage solutions: there are methods to create and drop namespaces, as well as methods to persist and retrieve objects.

**Sorted sets** - each set is identified with a unique string as the set name, and a number of entries, each associated with a numerical value as the weight of the entry. Functions on sets allow users to create and drop namespaces, as well as slice and paginate sets based on the weight of the entries.

These data structures can be used to solve variety of use-cases for applications; and they are straight forward to implement simply and efficiently on top of relational databases. Cantor provides this implementaion. It also tries to eliminate some of the complexities around relational databases, such as joins, constraints, and stored procedures. The aim of the library is to provide a simple and powerful set of abstractions for users to be able to spend more time on the application's business logic rather than the data storage and retrieval.
