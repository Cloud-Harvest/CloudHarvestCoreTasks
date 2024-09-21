# Caching
Data storage in Harvest is divided into two broad categories: the Ephemeral Cache and the Persistent Cache. 
The Ephemeral Cache is a Redis backend, and the Persistent Cache is a MongoDB backend.

# Table of Contents
1. [Ephemeral Cache](#ephemeral-cache)
2. [Persistent Cache](#persistent-cache)

# Ephemeral Cache
The ephemeral cache is a cache that is stored in a non-persistent storage. For Harvest, this means the Redis backend.
We call this a cache because we expect the data to be invalidated at some point in the future (ie job queues, tokens,
and agent/api node heartbeats).

We accept that this cache is ephemeral and that data stored in it may be lost at any time. In the event of a loss of the
cache, the system should be able to recover gracefully and continue to operate as expected once the cache layer as
been restarted.

At the time of writing, we have no plans to cache query results from our persistent storage in the ephemeral cache. This
is because we anticipate a combination of query tuning, indexing, configuration, and caching at the persistent layer to
be sufficient for our needs. We may revisit this decision in the future if we find that we need to cache query results
in the ephemeral cache.

| Database Name | Purpose                                                 | Expires After |
|---------------|---------------------------------------------------------|---------------|
| `api`         | Stores the API heartbeats.                              | 60 seconds    | 
| `agent`       | Stores the agent heartbeats.                            | 60 seconds    |
| `chains`      | Task chains that are currently.                         | 900 seconds   |
| `tokens`      | API, User, or other tokens are stored in this database. | Token expiry  |

# Persistent Cache
The persistent silo is a cache that is stored in a persistent storage. For Harvest, this means the MongoDb backend
database which supports the persisted storage of retrieved data. We call this a cache because we expect the data to be
invalidated at some point in the future (ie by the destruction of a cloud resource which was previously collected).

Furthermore, we anticipate that some historical data will be stored in this cache for a period until it is no longer
required. This is in contrast to the Redis cache which is used for temporary storage of data that is expected to be
invalidated in the near future.

| Database Name | Purpose                                                    |
|---------------|------------------------------------------------------------|
| `harvest`     | Stores the data that is collected by the Harvest system.   |
| `users`       | Stores persistent information about Harvest user accounts. |
