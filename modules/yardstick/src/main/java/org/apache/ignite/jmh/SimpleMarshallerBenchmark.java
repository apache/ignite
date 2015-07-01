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
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.jmh.model.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.results.*;
import org.openjdk.jmh.output.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@State
public class SimpleMarshallerBenchmark {

    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private Ignite node;

    private IgniteCache<Integer, PersonSimple> cache;

    private static final int ROWS_COUNT = 50000;

    private IgniteConfiguration createConfiguration(String gridName, Marshaller marshaller) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setMarshaller(marshaller);

        CacheConfiguration cCfg = new CacheConfiguration();
        cCfg.setName("query");
        cCfg.setCacheMode(CacheMode.PARTITIONED);
        cCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cCfg.setSwapEnabled(false);

        /*cCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cCfg.setOffHeapMaxMemory(0);
        cCfg.setSqlOnheapRowCacheSize(1);*/

        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(Integer.class);
        meta.setValueType(PersonSimple.class);

        HashMap<String, Class<?>> indexes = new HashMap<>();
        indexes.put("id", Integer.class);
        indexes.put("orgId", Integer.class);
        indexes.put("salary", Double.class);

        meta.setAscendingFields(indexes);

        HashMap<String, Class<?>> queryFields = new HashMap<>();
        queryFields.put("firstName", String.class);
        queryFields.put("lastName", String.class);

        meta.setQueryFields(queryFields);

        cCfg.setTypeMetadata(Arrays.asList(meta));

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    @TearDown
    public void stopNodes() {
        Ignition.stopAll(true);
    }

    //@Setup
    public void startOldNodes() {
        System.out.println();
        System.out.println("STARTED OLD NODE");

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1);

        node = Ignition.start(createConfiguration("node", marsh));

        cache = node.cache("query");

        for (int i = 0; i < ROWS_COUNT; i++) {
            PersonSimple person = new PersonSimple(i, "Name ", "Surname ", (i + 1) * 100);
            cache.put(i, person);
        }
    }

    @Setup
    public void startNewNodes() {
        System.out.println();
        System.out.println("STARTED NEW NODE");

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1_1);

        node = Ignition.start(createConfiguration("node", marsh));

        cache = node.cache("query");

        for (int i = 0; i < ROWS_COUNT; i++) {
            PersonSimple person = new PersonSimple(i, "Name ", "Surname ", (i + 1) * 100);
            cache.put(i, person);
        }
    }


    //@GenerateMicroBenchmark
    public void testQuery() throws Exception {
        double salary = ThreadLocalRandom.current().nextInt(0, ROWS_COUNT);

        double maxSalary = salary + 100;

        SqlQuery qry = new SqlQuery(PersonSimple.class, "salary >= ? and salary <= ?");

        qry.setArgs(salary, maxSalary);

        Collection<Cache.Entry<Integer, Object>> entries = cache.query(qry).getAll();

        entries.size();

//        Collection<Cache.Entry<Integer, PersonSimple>> entries = cache.query(qry).getAll();
//
//        for (Cache.Entry<Integer, PersonSimple> entry : entries) {
//            PersonSimple p = entry.getValue();
//
//            if (p.getSalary() < salary || p.getSalary() > maxSalary)
//                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
//                                        ", person=" + p + ']');
//        }
    }

    @GenerateMicroBenchmark
    public void testFieldsQuery() throws Exception {
        double salary = ThreadLocalRandom.current().nextInt(0, ROWS_COUNT);

        double maxSalary = salary + 100;

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT PersonSimple.firstName, PersonSimple.lastName FROM " +
            "PersonSimple WHERE salary >= ? and salary <= ?");

        qry.setArgs(salary, maxSalary);

        List<List<?>> result = cache.query(qry).getAll();

        for (List<?> row : result) {
            if (row.get(0) == null || row.get(1) == null)
                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                                        ", person=" + row + ']');
        }
    }

//    public static void main(String args[]) throws Exception {
//        SimpleMarshallerBenchmark benchmark = new SimpleMarshallerBenchmark();
//
//        benchmark.startOldNodes();
//
//        for (int i = 0; i < 500000000; i++) {
//            benchmark.testFieldsQuery();
//
//            if (i % 10000 == 0)
//                System.out.println("Iteration: " + i);
//        }
//
//        benchmark.stopNodes();
//    }


    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
            .include(".*" + SimpleMarshallerBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(15)
            .measurementIterations(35)
            .jvmArgs("-server")
            .forks(1)
            .outputFormat(OutputFormatType.TextReport)
            //.shouldDoGC(true)
            .build();

        new Runner(opts).run();
    }

}
