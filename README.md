<center>
![Ignite Logo](https://ignite.incubator.apache.org/images/logo3.png "Ignite Logo")
</center>

Apache Ignite In-Memory Data Fabric is a high-performance, integrated and distributed in-memory platform for computing and transacting on large-scale data sets in real-time, orders of magnitude faster than possible with traditional disk-based or flash technologies.
![Ignite Features](https://ignite.incubator.apache.org/images/apache-ignite.png "Ignite Features")

##Features
You can view Ignite as a collection of independent, well-integrated, in-memory components geared to improve performance and scalability of you application. Some of these components include:

  * Advanced Clustering
  * Compute Grid
  * Data Grid (JCache)
  * Service Grid
  * Ignite File System
  * Distributed Data Structures
  * Distributed Messaging
  * Distributed Events
  * Streaming & CEP
  * In-Memory Hadoop Accelerator

## Installation
Here is the quick summary on installation of Apache Ignite:
  * Download Apache Ignite as ZIP archive from https://ignite.incubator.apache.org/
  * Unzip ZIP archive into the installation folder in your system
  * Set `IGNITE_HOME` environment variable to point to the installation folder and make sure there is no trailing `/` in the path (this step is optional)

## Start From Command Line
An Ignite node can be started from command line either with default configuration or by passing a configuration file. You can start as many nodes as you like and they will all automatically discover each other.

### With Default Configuration
To start a grid node with default configuration, open the command shell and, assuming you are in `IGNITE_HOME` (Ignite installation folder), just type this:

	$ bin/ignite.sh

and you will see the output similar to this:

	[02:49:12] Ignite node started OK (id=ab5d18a6)
	[02:49:12] Topology snapshot [ver=1, nodes=1, CPUs=8, heap=1.0GB]

By default `ignite.sh` starts Ignite node with the default configuration: `config/default-config.xml`.

### Passing Configuration File
To pass configuration file explicitly,  from command line, you can type ggstart.sh <path to configuration file> from within your Ignite installation folder. For example:

	$ bin/ignite.sh examples/config/example-cache.xml

Path to configuration file can be absolute, or relative to either `IGNITE_HOME` (Ignite installation folder) or `META-INF` folder in your classpath.

### Interactive Mode
To pick a configuration file in interactive mode just pass `-i` flag, like so: `ignite.sh -i`.

## Get It With Maven
Another easy way to get started with Apache Ignite in your project is to use Maven 2 dependency management.

Ignite requires only one `ignite-core` mandatory dependency. Usually you will also need to add `ignite-spring` for spring-based XML configuration and `ignite-indexing` for SQL querying.

Replace `${ignite-version}` with actual Ignite version.

	<dependency>
	    <groupId>org.apache.ignite</groupId>
	    <artifactId>ignite-core</artifactId>
	    <version>${ignite.version}</version>
	</dependency>
	<dependency>
	    <groupId>org.apache.ignite</groupId>
	    <artifactId>ignite-spring</artifactId>
	    <version>${ignite.version}</version>
	</dependency>
	<dependency>
	    <groupId>org.apache.ignite</groupId>
	    <artifactId>ignite-indexing</artifactId>
	    <version>${ignite.version}</version>
	</dependency>

## First Ignite Compute Application
Let's write our first grid application which will count a number of non-white-space characters in a sentence. As an example, we will take a sentence, split it into multiple words, and have every compute job count number of characters in each individual word. At the end we simply add up results received from individual jobs to get our total count.

	try (Ignite ignite = Ignition.start()) {
	    Collection<GridCallable<Integer>> calls = new ArrayList<>();

	    // Iterate through all the words in the sentence and create Callable jobs.
	    for (final String word : "Count characters using callable".split(" ")) {
	        calls.add(new GridCallable<Integer>() {
	            @Override public Integer call() throws Exception {
	                return word.length();
	            }
	        });
	    }

	    // Execute collection of Callables on the grid.
	    Collection<Integer> res = ignite.compute().call(calls).get();

	    int sum = 0;

	    // Add up individual word lengths received from remote nodes.
	    for (int len : res)
	        sum += len;

	    System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
	}

### Zero Deployment
Note that when running above application from your IDE, remote nodes will execute received jobs without having your code on the class path. This is called Zero Deployment. Ignite automatically detects missing classes and loads class definitions from the nodes that have them.

## First Ignite Data Grid Application
Now let's write a simple set of mini-examples which will put and get values to/from distributed cache, and perform basic transactions.

Since we are using cache in this example, we should make sure that it is configured. Let's use example configuration shipped with Ignite that already has several caches configured:

	$ bin/ignite.sh examples/config/example-cache.xml

### Put and Get

	try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
	    IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

	    // Store keys in cache (values will end up on different cache nodes).
	    for (int i = 0; i < 10; i++)
	        cache.put(i, Integer.toString(i));

	    for (int i = 0; i < 10; i++)
	        System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
	}

### Atomic Operations

	// Put-if-absent which returns previous value.
	Integer oldVal = cache.getAndPutIfAbsent("Hello", 11);

	// Put-if-absent which returns boolean success flag.
	boolean success = cache.putIfAbsent("World", 22);

	// Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
	oldVal = cache.getAndReplace("Hello", 11);

	// Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
	success = cache.getAndReplace("World", 22);

	// Replace-if-matches operation.
	success = cache.replace("World", 2, 22);

	// Remove-if-matches operation.
	success = cache.remove("Hello", 1);

### Transactions

	try (Transaction tx = ignite.transactions().txStart()) {
	    Integer hello = cache.get("Hello");

	    if (hello == 1)
	        cache.put("Hello", 11);

	    cache.put("World", 22);

	    tx.commit();
	}

### Distributed Locks

	// Lock cache key "Hello".
	Lock lock = cache.lock("Hello");

	lock.lock();

	try {
	    cache.put("Hello", 11);
	    cache.put("World", 22);
	}
	finally {
	    lock.unlock();
	}

## Ignite Visor Admin Console
The easiest way to examine the content of the data grid as well as perform a long list of other management and monitoring operations is to use Ignite Visor Command Line Utility.

To start Visor simply run:

	$ bin/ignitevisorcmd.sh