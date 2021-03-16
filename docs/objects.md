# Objects

The `Objects` interface defines actions surrounding storing and retrieving key/value pairs. Keys are unique strings and the values are arbitrary byte arrays. This is consistent with other key/value storage solutions: there are methods to create and drop namespaces, as well as methods to persist and retrieve objects.




Use cases for the objects are effectively endless, storing key/value pairs is extremely common. Since values are arbitrary byte arrays, any content can be stored and retrieved using objects. Combining objects and sets can provide a large amount of utility, without spending a lot of time writing storage code. For example, create a set for the script name “email_admins.sh” with entries for each version, then store the script versions in objects.
