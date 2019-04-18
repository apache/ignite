GridGain Spark Module
---------------------------

GridGain provides an implementation of Spark RDD abstraction which enables easy access to GridGain caches.
GridGain RDD does not keep it's state in the memory of the Spark application and provides a view of the corresponding
GridGain cache. Depending on the chosen deployment mode this state may exist only during the lifespan of the Spark
application (embedded mode) or may exist outside of the Spark application (standalone mode), allowing seamless
sharing of the state between multiple Spark jobs.