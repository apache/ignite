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

create table PERSON(id integer not null, firstName varchar(50), lastName varchar(50), PRIMARY KEY(id));

insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');
insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');
insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');
insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');
insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');
insert into PERSON(id, first_name, last_name) values(6, 'Isaac', 'Newton');

The Ignite Schema Import utility generates the following artifacts:
 # Java POJO key and value classes
 # XML CacheTypeMetadata configuration
 # Java configuration snippet (alternative to XML)

After you exit from the wizard, you should:
 # Copy generated POJO java classes to you project source folder.
 # Copy XML declaration of CacheTypeMetadata to your Ignite XML configuration file under appropriate
  CacheConfiguration root.
 # Setup your Ignite XML configuration file DataSource to your RDBMS system for CacheJdbcPojoStore.
 # Or paste Java snippet with cache configuration into your Ignite initialization logic.
 # You need place compiled domain model classes, jdbc driver (used for connect to you RDBMS system) in Ignite node
  classpath, for example place in 'libs' folder.

Example of spring configuration:

<bean class="org.apache.ignite.configuration.IgniteConfiguration">
    ...
    <!-- Cache configuration. -->
    <property name="cacheConfiguration">
        <list>
            <bean class="org.apache.ignite.configuration.CacheConfiguration">
                ...

                <!-- Cache store. -->
                <property name="cacheStoreFactory">
                    <bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory">
                        <constructor-arg>
                            <bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore">
                                <property name="dataSource">
                                    <!-- TODO: Need specify connection pooling DataSource to your RDBMS system. -->
                                    ...
                                </property>
                            </bean>
                        </constructor-arg>
                    </bean>
                </property>

                <!-- Type mapping description. -->
                <property name="typeMetadata">
                    <list>
                        <bean class="org.apache.ignite.cache.CacheTypeMetadata">
                            <property name="databaseTable" value="PERSON"/>
                            <property name="keyType" value="org.apache.ignite.examples.datagrid.store.model.PersonKey"/>
                            <property name="valueType" value="org.apache.ignite.examples.datagrid.store.model.Person"/>
                            <property name="keyFields">
                                <list>
                                    <bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">
                                        <property name="databaseName" value="ID"/>
                                        <property name="databaseType">
                                            <util:constant static-field="java.sql.Types.BIGINT"/>
                                        </property>
                                        <property name="javaName" value="id"/>
                                        <property name="javaType" value="long"/>
                                    </bean>
                                </list>
                            </property>
                            <property name="valueFields">
                                <list>
                                    <bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">
                                        <property name="databaseName" value="ID"/>
                                        <property name="databaseType">
                                            <util:constant static-field="java.sql.Types.BIGINT"/>
                                        </property>
                                        <property name="javaName" value="id"/>
                                        <property name="javaType" value="long"/>
                                    </bean>
                                    <bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">
                                        <property name="databaseName" value="FIRST_NAME"/>
                                        <property name="databaseType">
                                            <util:constant static-field="java.sql.Types.VARCHAR"/>
                                        </property>
                                        <property name="javaName" value="firstName"/>
                                        <property name="javaType" value="java.lang.String"/>
                                    </bean>
                                    <bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">
                                        <property name="databaseName" value="LAST_NAME"/>
                                        <property name="databaseType">
                                            <util:constant static-field="java.sql.Types.VARCHAR"/>
                                        </property>
                                        <property name="javaName" value="lastName"/>
                                        <property name="javaType" value="java.lang.String"/>
                                    </bean>
                                </list>
                            </property>
                        </bean>
                    </list>
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

DataSource dataSource = null; // TODO: Need specify connection pooling DataSource to your RDBMS system.

// Create store.
CacheJdbcPojoStore store = new CacheJdbcPojoStore();
store.setDataSource(dataSource);

// Create store factory.
ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory<>(store));

// Configure cache to use store.
ccfg.setReadThrough(true);
ccfg.setWriteThrough(true);

// Configure cache types.
Collection<CacheTypeMetadata> meta = new ArrayList<>();

// PERSON type mapping.
CacheTypeMetadata tm = new CacheTypeMetadata();

tm.setDatabaseTable("PERSON");

tm.setKeyType("java.lang.Long");
tm.setValueType("org.apache.ignite.examples.datagrid.store.model.Person");

// Key fields for PERSONS.
tm.setKeyFields(F.asList(new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", Long.class)));

// Value fields for PERSONS.
tm.setValueFields(F.asList(
    new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", long.class),
    new CacheTypeFieldMetadata("FIRST_NAME", Types.VARCHAR, "firstName", String.class),
    new CacheTypeFieldMetadata("LAST_NAME", Types.VARCHAR, "lastName", String.class)
));
...
ccfg.setTypeMetadata(tm);

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
