

# MongoDB Java Server #

Fake implementation of the core [MongoDB][mongodb] server in Java that can be used for integration tests.

Think of H2/HSQLDB/SQLite but for MongoDB.

The [MongoDB Wire Protocol][wire-protocol] is implemented with [Netty][netty].
Different backends are possible and can be extended.

## In-Memory backend ##

The in-memory backend is the default backend that is typically used to fake MongoDB for integration tests.
It supports most CRUD operations, commands and the aggregation framework.
Some features are not yet implemented, such as full-text search or map/reduce.

Add the following Maven dependency to your project:

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server</artifactId>
    <version>1.18.2</version>
</dependency>
```

### Example ###

```java
public class SimpleTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @Before
    public void setUp() {
        server = new MongoServer(new MemoryBackend());

        // optionally: server.enableSsl(key, keyPassword, certificate);

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();

        client = new MongoClient(new ServerAddress(serverAddress));
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @After
    public void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.count());

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertEquals(1, collection.count());
        assertEquals(obj, collection.find().first());
    }

}
```

### Example with SpringBoot ###

```java
@RunWith(SpringRunner.class)
@SpringBootTest(classes={SimpleSpringBootTest.TestConfiguration.class})
public class SimpleSpringBootTest {

    @Autowired private MyRepository repository;

    @Before
    public void setUp() {
        // initialize your repository with some test data
        repository.deleteAll();
        repository.save(...);
    }

    @Test
    public void testMyRepository() {
        // test your repository ...
        ...
    }

    @Configuration
    @EnableMongoRepositories(basePackageClasses={MyRepository.class})
    protected static class TestConfiguration {
        @Bean
        public MongoTemplate mongoTemplate(MongoClient mongoClient) {
            return new MongoTemplate(mongoDbFactory(mongoClient));
        }

        @Bean
        public MongoDbFactory mongoDbFactory(MongoClient mongoClient) {
            return new SimpleMongoDbFactory(mongoClient, "test");
        }

        @Bean(destroyMethod="shutdown")
        public MongoServer mongoServer() {
            MongoServer mongoServer = new MongoServer(new MemoryBackend());
            mongoServer.bind();
            return mongoServer;
        }

        @Bean(destroyMethod="close")
        public MongoClient mongoClient(MongoServer mongoServer) {
            return new MongoClient(new ServerAddress(mongoServer.getLocalAddress()));
        }
    }
}
```

## H2 MVStore backend ##

The [H2 MVStore][h2-mvstore] backend connects the server to a `MVStore` that
can either be in-memory or on-disk.

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server-h2-backend</artifactId>
    <version>1.18.2</version>
</dependency>
```

### Example ###

```java
public class Application {

    public static void main(String[] args) throws Exception {
        MongoServer server = new MongoServer(new H2Backend("database.mv"));
        server.bind("localhost", 27017);
    }

}
```

## PostgreSQL backend ##

The PostgreSQL backend is a proof-of-concept implementation that connects the server to a database in a running
PostgreSQL 9.5+ instance. Each MongoDB database is mapped to a schema in
Postgres and each MongoDB collection is stored as a table.

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server-postgresql-backend</artifactId>
    <version>1.18.2</version>
</dependency>
```

For integration tests, a PostgreSQL instance can be created in a docker container:

```
$ docker-compose up -d
```

or manually with:

```
$ docker run --name postgres-mongo-java-server-test \
             -p 5432:5432 \
             --tmpfs /var/lib/postgresql/data:rw \
             -e POSTGRES_USER=mongo-java-server-test \
             -e POSTGRES_PASSWORD=mongo-java-server-test \
             -e POSTGRES_DB=mongo-java-server-test \
             -d postgres:9.6-alpine
```


### Example ###

```java
public class Application {

    public static void main(String[] args) throws Exception {
        DataSource dataSource = new org.postgresql.jdbc3.Jdbc3PoolingDataSource();
        dataSource.setDatabaseName(…);
        dataSource.setUser(…);
        dataSource.setPassword(…);
        MongoServer server = new MongoServer(new PostgresqlBackend(dataSource));
        server.bind("localhost", 27017);
    }

}
```

## Contributing ##

Please read the [contributing guidelines](CONTRIBUTING.md) if you want to contribute code to the project.

If you want to thank the author for this library or want to support the maintenance work, we are happy to receive a donation.

[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.me/BenediktWaldvogel)

## Ideas for other backends ##

### Faulty backend ###

A faulty backend could randomly fail queries or cause timeouts. This could be
used to test the client for error resilience.

### Fuzzy backend ###

Fuzzing the wire protocol could be used to check the robustness of client
drivers.

## Related Work ##

* [Embedded MongoDB][embedded-mongodb]
  * Spins up a real MongoDB instance

* [fongo][fongo]
  * focus on unit testing
  * no wire protocol implementation
  * intercepts the java mongo driver
  * currently used in [nosql-unit][nosql-unit]

[mongodb]: http://www.mongodb.org/
[wire-protocol]: https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/
[netty]: http://netty.io/
[embedded-mongodb]: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
[fongo]: https://github.com/fakemongo/fongo
[nosql-unit]: https://github.com/lordofthejars/nosql-unit
[h2-mvstore]: http://www.h2database.com/html/mvstore.html
