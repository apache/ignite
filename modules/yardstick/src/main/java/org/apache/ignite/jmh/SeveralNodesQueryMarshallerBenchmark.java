/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.jmh.model.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.output.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import javax.cache.*;
import java.util.*;

/**
 * Created by GridAdmin1234 on 6/25/2015.
 */
@State (value = Scope.Benchmark)
public class SeveralNodesQueryMarshallerBenchmark {

    private final static String PERSON_CACHE = "person";

    private final static String CITY_CACHE = "city";

    private final static String DEPARTMENT_CACHE = "dep";

    private final static String ORGANIZATION_CACHE = "org";

    private static final int PERSON_ROWS_COUNT = 50000;

    private static final int CITY_ROWS_COUNT = 500;

    private static final int DEPARTMENT_ROWS_COUNT = CITY_ROWS_COUNT * 10;

    private static final int ORGANIZATION_ROWS_COUNT = 30;

    private static final int SALARY_LIMIT = 100000;


    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private Ignite clientNode;

    private IgniteCache<AffinityKey<UUID>, Person> personCache;

    private IgniteConfiguration createOldConfiguration(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        OptimizedMarshaller marshaller = new OptimizedMarshaller();
        marshaller.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1);

        cfg.setMarshaller(marshaller);

        CacheConfiguration cityCacheCfg = createCacheCfg(CITY_CACHE);
        cityCacheCfg.setTypeMetadata(Arrays.asList(createCityMetadata()));

        CacheConfiguration orgCacheCfg = createCacheCfg(ORGANIZATION_CACHE);
        orgCacheCfg.setTypeMetadata(Arrays.asList(createOrgMetadata()));

        CacheConfiguration depCacheCfg = createCacheCfg(DEPARTMENT_CACHE);
        depCacheCfg.setTypeMetadata(Arrays.asList(createDepartmentMeta()));

        CacheConfiguration personCacheCfg = createCacheCfg(PERSON_CACHE);
        personCacheCfg.setTypeMetadata(Arrays.asList(createPersonMeta()));

        cfg.setCacheConfiguration(cityCacheCfg, orgCacheCfg, depCacheCfg, personCacheCfg);

//        CacheConfiguration cityCacheCfg = createCacheCfg(CITY_CACHE);
//        cityCacheCfg.setIndexedTypes(UUID.class, City.class);
//
//        CacheConfiguration orgCacheCfg = createCacheCfg(ORGANIZATION_CACHE);
//        orgCacheCfg.setIndexedTypes(UUID.class, Organization.class);
//
//        CacheConfiguration depCacheCfg = createCacheCfg(DEPARTMENT_CACHE);
//        depCacheCfg.setIndexedTypes(AffinityKey.class, Department.class);
//
//        CacheConfiguration personCacheCfg = createCacheCfg(PERSON_CACHE);
//        personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);
//
//        cfg.setCacheConfiguration(cityCacheCfg, orgCacheCfg, depCacheCfg, personCacheCfg);

        return cfg;
    }

    private IgniteConfiguration createNewConfiguration(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        OptimizedMarshaller marshaller = new OptimizedMarshaller();
        marshaller.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1_1);
        cfg.setMarshaller(marshaller);

        CacheConfiguration cityCacheCfg = createCacheCfg(CITY_CACHE);
        cityCacheCfg.setTypeMetadata(Arrays.asList(createCityMetadata()));

        CacheConfiguration orgCacheCfg = createCacheCfg(ORGANIZATION_CACHE);
        orgCacheCfg.setTypeMetadata(Arrays.asList(createOrgMetadata()));

        CacheConfiguration depCacheCfg = createCacheCfg(DEPARTMENT_CACHE);
        depCacheCfg.setTypeMetadata(Arrays.asList(createDepartmentMeta()));

        CacheConfiguration personCacheCfg = createCacheCfg(PERSON_CACHE);
        personCacheCfg.setTypeMetadata(Arrays.asList(createPersonMeta()));

        cfg.setCacheConfiguration(cityCacheCfg, orgCacheCfg, depCacheCfg, personCacheCfg);

        return cfg;
    }

    private CacheConfiguration createCacheCfg(String name) {
        CacheConfiguration cCfg = new CacheConfiguration();
        cCfg.setName(name);
        cCfg.setCacheMode(CacheMode.PARTITIONED);
        cCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cCfg.setSwapEnabled(false);

//        cCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
//        cCfg.setOffHeapMaxMemory(0);
//        cCfg.setSqlOnheapRowCacheSize(1);

        return cCfg;
    }

    private CacheTypeMetadata createCityMetadata() {
        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(UUID.class);
        meta.setValueType(City.class);

        HashMap<String, Class<?>> indexes = new HashMap<>();
        indexes.put("id", UUID.class);

        HashMap<String, Class<?>> queryFields = new HashMap<>();
        queryFields.put("name", String.class);
        queryFields.put("population", Integer.class);
        queryFields.put("age", Integer.class);

        meta.setAscendingFields(indexes);
        meta.setQueryFields(queryFields);

        return meta;
    }

    private CacheTypeMetadata createOrgMetadata() {
        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(UUID.class);
        meta.setValueType(Organization.class);

        HashMap<String, Class<?>> indexes = new HashMap<>();
        indexes.put("id", UUID.class);

        HashMap<String, Class<?>> queryFields = new HashMap<>();
        queryFields.put("name", String.class);

        meta.setAscendingFields(indexes);
        meta.setQueryFields(queryFields);

        return meta;
    }

    private CacheTypeMetadata createDepartmentMeta() {
        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(AffinityKey.class);
        meta.setValueType(Department.class);

        HashMap<String, Class<?>> indexes = new HashMap<>();
        indexes.put("id", UUID.class);
        indexes.put("cityId", UUID.class);
        indexes.put("orgId", UUID.class);

        HashMap<String, Class<?>> queryFields = new HashMap<>();
        queryFields.put("name", String.class);

        meta.setAscendingFields(indexes);
        meta.setQueryFields(queryFields);

        return meta;
    }

    private CacheTypeMetadata createPersonMeta() {
        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(AffinityKey.class);
        meta.setValueType(Person.class);

        HashMap<String, Class<?>> indexes = new HashMap<>();
        indexes.put("id", UUID.class);
        indexes.put("depId", UUID.class);

        HashMap<String, Class<?>> queryFields = new HashMap<>();
        queryFields.put("firstName", String.class);
        queryFields.put("lastName", String.class);
        queryFields.put("rank", Integer.class);
        queryFields.put("title", String.class);
        queryFields.put("age", Integer.class);
        queryFields.put("salary", Integer.class);

        meta.setAscendingFields(indexes);
        meta.setQueryFields(queryFields);

        return meta;
    }

    private void populateCaches() {
        Random rand = new Random();

        System.out.println();

        IgniteCache<UUID, City> cityCache = clientNode.cache(CITY_CACHE);

        City[] cities = new City[CITY_ROWS_COUNT];

        for (int i = 0; i < CITY_ROWS_COUNT; i++) {
            City city = new City("City Name " + i, rand.nextInt(10000000), rand.nextInt(2000), "Country" + i);

            cities[i] = city;

            cityCache.put(city.getId(), city);
        }

        System.out.println("Populated cities: " + CITY_ROWS_COUNT);

        IgniteCache<UUID, Organization> orgCache = clientNode.cache(ORGANIZATION_CACHE);

        Organization[] organizations = new Organization[ORGANIZATION_ROWS_COUNT];

        for (int i = 0; i < ORGANIZATION_ROWS_COUNT; i++) {
            Organization org = new Organization("Org Name " + i);

            organizations[i] = org;

            orgCache.put(org.getId(), org);
        }

        System.out.println("Populated organizations: " + ORGANIZATION_ROWS_COUNT);

        IgniteCache<AffinityKey<UUID>, Department> depCache = clientNode.cache(DEPARTMENT_CACHE);

        Department[] departments = new Department[DEPARTMENT_ROWS_COUNT];

        for (int i = 0; i < DEPARTMENT_ROWS_COUNT; i++) {
            Department dep = new Department("Dep Name " + i, cities[rand.nextInt(CITY_ROWS_COUNT)],
                organizations[rand.nextInt(ORGANIZATION_ROWS_COUNT)]);

            departments[i] = dep;

            try {
                depCache.put(dep.getKey(), dep);
            }
            catch (Exception e) {
                System.out.println("ROW: " + i);
                e.printStackTrace();
            }
        }

        System.out.println("Populated departments: " + DEPARTMENT_ROWS_COUNT);

        cities = null;
        organizations = null;

        personCache = clientNode.cache(PERSON_CACHE);

        for (int i = 0; i < PERSON_ROWS_COUNT; i++) {
            Person person = new Person(departments[rand.nextInt(DEPARTMENT_ROWS_COUNT)], "First Name " + i,
                "Last Name " + i, rand.nextInt(21), "Title " + i, rand.nextInt(90), rand.nextInt(SALARY_LIMIT));

            personCache.put(person.getKey(), person);
        }

        System.out.println("Populated persons: " + PERSON_ROWS_COUNT);

        System.out.printf("");
    }

    //@Setup (Level.Trial)
    public void startOldNodes() {
        System.out.println();

        System.out.println("Using OLD marshaller");

        Ignition.start(createOldConfiguration("server1"));
        Ignition.start(createOldConfiguration("server2"));
        Ignition.start(createOldConfiguration("server3"));

        IgniteConfiguration clientCfg = createOldConfiguration("client");
        clientCfg.setClientMode(true);

        clientNode = Ignition.start(clientCfg);

        populateCaches();
    }

    @Setup (Level.Trial)
    public void startNewNodes() {
        System.out.println();

        System.out.println("Using NEW marshaller");

        Ignition.start(createNewConfiguration("server1"));
        Ignition.start(createNewConfiguration("server2"));
        Ignition.start(createNewConfiguration("server3"));

        IgniteConfiguration clientCfg = createNewConfiguration("client");
        clientCfg.setClientMode(true);

        clientNode = Ignition.start(clientCfg);

        populateCaches();
    }

    @TearDown (Level.Trial)
    public void stopNodes() {
        Ignition.stopAll(true);
    }

    //@GenerateMicroBenchmark
    public void sqlQuerySimple() {
        String sql = "salary > ? and salary <= ?";

        List<Cache.Entry<AffinityKey<UUID>, Person>> result =
            personCache.query(new SqlQuery<AffinityKey<UUID>, Person>(Person.class, sql).
            setArgs(0, 200000)).getAll();

        if (result.size() == 0)
            throw new RuntimeException("Invalid result size");
    }

    //@GenerateMicroBenchmark
    public void sqlQueryWithJoin() {
        // SQL clause query which joins on 2 types to select people for a specific organization.
        String joinSql =
            "from Person, \"" + DEPARTMENT_CACHE + "\".Department as dep " +
                "where Person.depId = dep.id " +
                "and lower(dep.name) = lower(?)";

        // Execute queries for find employees for different organizations.
        List<Cache.Entry<AffinityKey<UUID>, Person>> result =
            personCache.query(new SqlQuery<AffinityKey<UUID>, Person>(Person.class, joinSql).
                setArgs("Dep Name 1")).getAll();

        if (result.size() == 0)
            throw new RuntimeException("Invalid result size");
    }

    @GenerateMicroBenchmark
    public void sqlQueryWithAggregation() {
        // Calculate average of salary of all persons in ApacheIgnite.
        // Note that we also join on Organization cache as well.
        Random rand = new Random();

        String sql =
//            "select Person.firstName, Person.lastName, Person.salary " +
            "select Person.firstName, city.name " +
                "from Person, \"" + DEPARTMENT_CACHE + "\".Department as dep, " +
                "\"" + CITY_CACHE + "\".City as city " +
                "where Person.depId = dep.id and dep.cityId = city.id " +
                "and city.population > ? and city.population <= ?";

        QueryCursor<List<?>> cursor = personCache.query(new SqlFieldsQuery(sql).setArgs(rand.nextInt(1000),
                                                                                        rand.nextInt(10000000)));

        if (cursor.getAll().size() == 0)
            System.err.println("Invalid result size");
    }


    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
            .include(".*" + SeveralNodesQueryMarshallerBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(15)
            .measurementIterations(35)
            .jvmArgs("-server")
            .forks(1)
            .outputFormat(OutputFormatType.TextReport)
            .build();

        new Runner(opts).run();
    }

//    public static void main(String... args) throws Exception {
//        SeveralNodesQueryMarshallerBenchmark benchmark = new SeveralNodesQueryMarshallerBenchmark();
//
//        benchmark.startNewNodes();
//
//        for (int i = 0; i < 20000; i++) {
//            benchmark.sqlQueryWithAggregation();
//
//            if (i != 0 && i % 2000  == 0) {
//                System.out.println("Iteration: " + i);
//                break;
//            }
//        }
//
//        //benchmark.stopNodes();
//    }
}
