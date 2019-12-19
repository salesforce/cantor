# Sets

The `Sets` interface defines a series of actions surrounding sorted sets. Each set is identified with a unique string as the set name, and a number of entries, each associated with a numerical value as the weight of the entry. Functions on sets allow users to create and drop namespaces, as well as slice and paginate sets based on the weight of the entries.

Sets are commonly used to store relationships and memberships of corresponding stored values. Set names can be the names of the membership groups (“admins” or “users”), and the entries can be user ids or email addresses, with weight being the timestamp of when they were added to the group. Another common use case is using the set as a work queue, the set name corresponding to the name of the job (“customer-report” or “data-compaction”) and the entries can be ids for the job. Using the `pop(...)` method allows atomically retrieving an id, ensuring the corresponding job is only run once.

