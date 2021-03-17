Majority of applications require some form of persistence. The data access object layer implementation usually accounts for a considerable portion of the code in these applications. This layer usually contains code to initalize and connect to some storage system; a mapping to/from the layout of the data in storage and the corresponding representation in the application; and also, composing and executing queries against the storage, handling edge cases, handling exceptions, etc. This is where Cantor can help to reduce the code and its complexity.

Some of the commonly used patterns to access data are:

- Store and retrieve single objects; for example, storing user informations. The data stored is usually relatively small, it can be a JSON object, or a serialized protobuf object, or any other form of data that can be converted to a byte array. Key/value storages such as BerkeleyDB or Redis are usually used for this purpose.

- Store and retrieve collections of data; for example, list of users in a certain group. Relational databases or indexing engines are usually used for this purpose.

- Store and retireve temporal data points; for example, metric values, or IoT events. Time-series databases such as Elastic Search, InfluxDB, OpenTSDB, or Prometheus are used for this purpose.

Cantor tries to provide a set of simple yet powerful abstractions that can be used to address essential needs for the above mentioned use cases. **The implementation focues more on simplicity and usability than completeness, performance, or scale.** This is not a suitable solution for large scale (data sets larger than few tera-bytes) applications. It is also not recommended for high throughput (more than a hundred thousand operations per second) applications.
