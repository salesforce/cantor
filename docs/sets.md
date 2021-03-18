## Definition

The `Sets` interface defines a series of actions surrounding sorted sets of strings. Each set is identified with a unique string as the set name, and a number of string entries, each associated with a numerical value as the weight of the entry. Functions on sets allow users to create and drop namespaces, as well as slice and paginate sets based on the weight of the entries.

A set looks like this:
```json
{
	"set1": {
		"entry1": 0,
		"entry2": 5,
		"entry3": 10
	}
}
```

## HTTP API

To make HTTP calls in local testing environment, use this base URL: [http://localhost:8084](http://localhost:8084).

For convenience, you can use [Cantor Swagger UI](http://localhost:8084), which comes with your local cantor instance, to compose the full custom URL for your API calls. Full URL to each API endpoint's Swagger UI page is linked on each endpoint below. **Remember to spin up your local cantor HTTP server instance before you click on any Swagger UI link on this page.**

Most of the `Sets` API endpoints need required and/or optional URL parameters. Required URL parameters are shown as part of endpoint's path, while optional URL parameters, if existed, are given below.

### [GET ​/api​/sets](http://localhost:8084/#/Sets%20Resource/getNamespaces_2)

Get all set namespaces.

**Sample Code:**

```bash
curl -X GET "http://localhost:8084/api/sets" -H "accept: application/json"
```

### [GET ​/api​/sets​/{namespace}](http://localhost:8084/#/Sets%20Resource/sets)

Get list of all sets in a namespace.

**Sample Code:**

This HTTP call returns all sets in the namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/sets/dev" -H "accept: application/json"
```

### [GET /api​/sets​/{namespace}​/{set}](http://localhost:8084/#/Sets%20Resource/get)

Get entries from a set.

**Optional URL Paramemter(s):**

- `min`: `integer`

	Minimum weight for an entry.

- `max`: `integer`
	
	Maximum weight for an entry.

- `start`: `integer`
	
	Index from which to start counting.

- `count`: `integer`

	Number of entries allowed in response.

- `asc`: `boolean`

	Return in ascending or descending format.

**Sample Code:**

This HTTP call returns the top 10 entries with the lightest weights from the set `set1` under the namespace `dev`, with weights ranging from 1 to 100.

```bash
curl -X GET "http://localhost:8084/api/sets/dev/set1?min=1&max=100&start=0&count=10&asc=true" -H "accept: application/json"
```

### [GET ​/api​/sets​/size​/{namespace}​/{set}](http://localhost:8084/#/Sets%20Resource/size_1)

Get number of entries in a set.

**Sample Code:**

This HTTP call returns the number of the entries in the set `set1` under the namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/sets/size/dev/set1" -H "accept: application/json"
```

### [GET /api​/sets​/entries​/{namespace}​/{set}](http://localhost:8084/#/Sets%20Resource/entries)

Get entry names from a set.

**Optional URL Paramemter(s):**

- `min`: `integer`

	Minimum weight for an entry.

- `max`: `integer`
	
	Maximum weight for an entry.

- `start`: `integer`
	
	Index from which to start counting.

- `count`: `integer`

	Number of entries allowed in response.

- `asc`: `boolean`

	Return in ascending or descending format.

**Sample Code:**

This HTTP call returns the names of the top 10 entries with the lightest weights from the set `set1` under the namespace `dev`, with weights ranging from 1 to 100.

```bash
curl -X GET "http://localhost:8084/api/sets/entries/dev/set1?min=1&max=100&start=0&count=10&asc=true" -H "accept: application/json"
```

### [GET /api​/sets​/weight​/{namespace}​/{set}​/{entry}](http://localhost:8084/#/Sets%20Resource/weight)

Get weight of a specific entry in a set.

**Sample Code:**

This HTTP call returns the weight for the entry `entry1` in the set `set1` under the namespace `dev`.

```bash
curl -X GET "http://localhost:8084/api/sets/weight/dev/set1/entry1" -H "accept: application/json"
```

### [GET /api​/sets​/union​/{namespace}](http://localhost:8084/#/Sets%20Resource/union)

Perform a union of all provided sets.

**Optional URL Paramemter(s):**

- `set`: `array[string]`

	List of sets.

- `min`: `integer`

	Minimum weight for an entry.

- `max`: `integer`
	
	Maximum weight for an entry.

- `start`: `integer`
	
	Index from which to start counting.

- `count`: `integer`

	Number of entries allowed in response.

- `asc`: `boolean`

	Return in ascending or descending format.

**Sample Code:**

```bash
```

### [GET /api​/sets​/intersect​/{namespace}](http://localhost:8084/#/Sets%20Resource/intersect)

Perform an intersection of all provided sets.

**Sample Code:**

```bash
```

### [POST ​/api​/sets​/{namespace}​/{set}​/{entry}​/{count}](http://localhost:8084/#/Sets%20Resource/inc)

Atomically increment the weight of an entry and return the value after increment.

**Sample Code:**

```bash
```

### [PUT /api​/sets​/{namespace}](http://localhost:8084/#/Sets%20Resource/create_2)

Create a new set namespace.

**Sample Code:**

```bash
```

### [PUT ​/api​/sets​/{namespace}​/{set}​/{entry}​/{weight}](http://localhost:8084/#/Sets%20Resource/add)

Add or overwrite an entry in a set.

**Sample Code:**

```bash
```

### [DELETE ​/api​/sets​/{namespace}](http://localhost:8084/#/Sets%20Resource/drop_2)

Drop a set namespace.

**Sample Code:**

```bash
```

### [DELETE /api​/sets​/{namespace}​/{set}](http://localhost:8084/#/Sets%20Resource/delete_1)

Delete entries in a set between provided weights.

**Sample Code:**

```bash
```

### [DELETE /api​/sets​/pop​/{namespace}​/{set}](http://localhost:8084/#/Sets%20Resource/pop)

Pop entries from a set.

**Sample Code:**

```bash
```

### [DELETE ​/api​/sets​/{namespace}​/{set}​/{entry}](http://localhost:8084/#/Sets%20Resource/delete)

Delete a specific entry by name.

**Sample Code:**

```bash
```

## Java gRPC API

## Use case

Sets are commonly used to store relationships and memberships of corresponding stored values. Set names can be the names of the membership groups (“admins” or “users”), and the entries can be user ids or email addresses, with weight being the timestamp of when they were added to the group. Another common use case is using the set as a work queue, the set name corresponding to the name of the job (“customer-report” or “data-compaction”) and the entries can be ids for the job. Using the `pop(...)` method allows atomically retrieving an id, ensuring the corresponding job is only run once.
