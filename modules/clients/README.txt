Java Client README
==================
Java Client is a lightweight gateway to Ignite nodes.

Client communicates with grid nodes via REST interface and provides reduced but powerful subset of Ignite API.
Java Client allows to use Ignite features from devices and environments where fully-functional Ignite node
could not (or should not) be started.

Client vs Grid Node
===================
Note that for performance and ease-of-use reasons, you should always prefer to start grid node in your cluster
instead of remote client. Grid node will generally perform a lot faster and can easily exhibit client-only
functionality by excluding it from task/job execution and from caching data.

For example, you can prevent a grid node from participating in caching by setting
`CacheConfiguration.setDistributionMode(...)` value to either `CLIENT_ONLY` or `NEAR_ONLY`.
