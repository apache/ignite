Apache Ignite Schema Import Module
------------------------------------------

Ignite ships with its own database schema mapping wizard which provides automatic support for integrating with
persistence stores. This utility automatically connects to the underlying database and generates all the required
XML OR-mapping configuration and Java domain model POJOs.

To start the wizard for generating database schema mapping, execute bin/ignite-schema-import.sh script:

For connection with RDBMS system from utility you need to provide: connection url and jdbc driver.
Note that JDBC drivers are not supplied with the utility and should be provided separately.

Moving from disk-based architectures to in-memory architectures
------------------------------------------

Use Schema Import Utility for generation of type mapping and domain model in Java.

For example you may use the following script for create sample type 'Person' in your RDBMS system:

create table PERSON(id integer not null PRIMARY KEY, first_name varchar(50), last_name varchar(50), salary double);

insert into PERSON(id, first_name, last_name, salary) values(1, 'Johannes', 'Kepler', 1000);
insert into PERSON(id, first_name, last_name, salary) values(2, 'Galileo', 'Galilei', 1200);
insert into PERSON(id, first_name, last_name, salary) values(3, 'Henry', 'More', 1150);
insert into PERSON(id, first_name, last_name, salary) values(4, 'Polish', 'Brethren', 2000);
insert into PERSON(id, first_name, last_name, salary) values(5, 'Robert', 'Boyle', 2500);
insert into PERSON(id, first_name, last_name, salary) values(6, 'Isaac', 'Newton', 1300);

The Ignite Schema Import utility generates the following artifacts:
 # Java POJO key and value classes (enter "org.apache.ignite.schema" package name before generation).
 # XML CacheTypeMetadata configuration.
 # Java configuration snippet (alternative to XML).

After you exit from the wizard, you should:
 # Copy generated POJO java classes to you project source folder.
 # Copy XML declaration of CacheTypeMetadata to your Ignite XML configuration file under appropriate
  CacheConfiguration root.
 # Setup your Ignite XML configuration file DataSource to your RDBMS system for CacheJdbcPojoStore.
 # Or paste Java snippet with cache configuration into your Ignite initialization logic.
 # You need place compiled domain model classes, jdbc driver (used for connect to you RDBMS system) in Ignite node
  classpath, for example place in 'libs' folder.

Example of spring configuration:

<!-- Sample data source. -->
<bean id="myDataSource" class="org.h2.jdbcx.JdbcDataSource"/>

<bean class="org.apache.ignite.configuration.IgniteConfiguration">
    ...
    <!-- Cache configuration. -->
    <property name="cacheConfiguration">
        <list>
            <bean class="org.apache.ignite.configuration.CacheConfiguration">
                ...

                <!-- Cache store. -->
                <property name="cacheStoreFactory">
                    <bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory">
                        <property name="dataSourceBean" value="myDataSource"/>
                        <bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore">
                            <property name="dataSourceBean" value="myDataSource" />
                            <property name="types">
                                <list>
                                    <bean class="org.apache.ignite.cache.store.jdbc.JdbcType">
                                        <property name="cacheName" value="myCache" />
                                        <property name="databaseSchema" value="MY_DB_SCHEMA" />
                                        <property name="databaseTable" value="PERSON" />
                                        <property name="keyType" value="java.lang.Integer" />
                                        <property name="keyFields">
                                            <list>
                                                <bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">
                                                    <property name="databaseFieldType" >
                                                        <util:constant static-field="java.sql.Types.INTEGER"/>
                                                    </property>
                                                    <property name="databaseFieldName" value="ID" />
                                                    <property name="javaFieldType" value="java.lang.Integer" />
                                                    <property name="javaFieldName" value="id" />
                                                </bean>
                                            </list>
                                        </property>
                                        <property name="valueType" value="my.company.Person" />
                                        <property name="valueFields">
                                            <list>
                                                <bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">
                                                    <property name="databaseFieldType" >
                                                        <util:constant static-field="java.sql.Types.VARCHAR"/>
                                                    </property>
                                                    <property name="databaseFieldName" value="first_name" />
                                                    <property name="javaFieldType" value="java.lang.String" />
                                                    <property name="javaFieldName" value="firstName" />
                                                </bean>
                                                <bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">
                                                    <property name="databaseFieldType" >
                                                        <util:constant static-field="java.sql.Types.VARCHAR"/>
                                                    </property>
                                                    <property name="databaseFieldName" value="last_name" />
                                                    <property name="javaFieldType" value="java.lang.String" />
                                                    <property name="javaFieldName" value="lastName" />
                                                </bean>
                                                <bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">
                                                    <property name="databaseFieldType" >
                                                        <util:constant static-field="java.sql.Types.DOUBLE"/>
                                                    </property>
                                                    <property name="databaseFieldName" value="salary" />
                                                    <property name="javaFieldType" value="java.lang.Double" />
                                                    <property name="javaFieldName" value="salary" />
                                                </bean>
                                            </list>
                                        </property>
                                    </bean>
                                </list>
                            </property>
                        </bean>
                    </bean>
                </property>
                ...
            </bean>
        </list>
    </property>
    ...
</bean>

Example of java code configuration:

IgniteConfiguration cfg = new IgniteConfiguration();
...
CacheConfiguration ccfg = new CacheConfiguration<>();

// Create store factory.
CacheJdbcPojoStoreFactory storeFactory = new CacheJdbcPojoStoreFactory();
storeFactory.setDataSourceBean("myDataSource");

// Configure cache types.
Collection<JdbcType> jdbcTypes = new ArrayList<>();

// PERSON type mapping.
JdbcType jdbcType = new JdbcType();

jdbcType.setCacheName(CACHE_NAME);

jdbcType.setDatabaseSchema("MY_DB_SCHEMA");
jdbcType.setDatabaseTable("PERSON");

jdbcType.setKeyType("java.lang.Integer");
jdbcType.setValueType("my.company.Person");

// Key fields for PERSONS.
jdbcType.setKeyFields(F.asArray(new JdbcType(Types.INTEGER, "ID", Integer.class, "id")));

// Value fields for PERSONS.
jdbcType.setValueFields(F.asArray(
    new JdbcType(Types.INTEGER, "ID", int.class, "id"),
    new JdbcType(Types.VARCHAR, "first_name", String.class, "firstName"),
    new JdbcType(Types.VARCHAR, "last_name", String.class, "lastName"),
    new JdbcType(Types.DOUBLE, "salary", Double.class, "salary")
));

storeFactory.setTypes(jdbcTypes.toArray(new JdbcType[]));

// Configure cache to use store.
ccfg.setReadThrough(true);
ccfg.setWriteThrough(true);
ccfg.setCacheStoreFactory(storeFactory);

cfg.setCacheConfiguration(ccfg);

...

// Start Ignite node.
Ignition.start(cfg);

Now you can load all data from database to cache:
    IgniteCache<Long, Person> cache = ignite.jcache(CACHE_NAME);
    cache.loadCache(null);

Or you can load data from database to cache with custom SQL:
    cache.loadCache(null, "java.lang.Long", "select * from PERSON where id = 2")

Also if you put data into cache it will be inserted / updated in underlying database.


Performance optimization.
------------------------------------------

1. Use DataSource with connection pool.
2. Enable write-behind feature by default write-behind is disabled.
   Note, write-behind should not be used with TRANSACTIONAL caches.

Example of spring configuration:

<bean class="org.apache.ignite.configuration.IgniteConfiguration">
    ...
    <!-- Cache configuration. -->
    <property name="cacheConfiguration">
        <list>
            <bean class="org.apache.ignite.configuration.CacheConfiguration">
                ...
                <!-- Sets flag indicating whether write-behind is enabled.. -->
                <property name="writeBehindEnabled" value="true/>
                ...
            </bean>
        </list>
    </property>
    ...
</bean>

Example of java code configuration:

IgniteConfiguration cfg = new IgniteConfiguration();
...
CacheConfiguration ccfg = new CacheConfiguration<>();
...
ccfg.setWriteBehindEnabled(true);
...
// Start Ignite node.
Ignition.start(cfg);
