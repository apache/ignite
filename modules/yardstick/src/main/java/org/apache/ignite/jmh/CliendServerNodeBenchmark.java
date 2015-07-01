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
import org.openjdk.jmh.output.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by GridAdmin1234 on 6/29/2015.
 */
@State (value = Scope.Benchmark)
public class CliendServerNodeBenchmark {
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private Ignite node;

    private IgniteCache<Integer, PersonSimple> cache;

    private static final int ROWS_COUNT = 50000;

    private int putCounter = ROWS_COUNT + 1;

    private IgniteConfiguration createConfiguration(String gridName, boolean newMarshaller) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        OptimizedMarshaller marsh = new OptimizedMarshaller(false);
        marsh.setProtocolVersion(newMarshaller ? OptimizedMarshallerProtocolVersion.VER_1_1 :
                                     OptimizedMarshallerProtocolVersion.VER_1);
        cfg.setMarshaller(marsh);

        CacheConfiguration cCfg = new CacheConfiguration();
        cCfg.setName("query");
        cCfg.setCacheMode(CacheMode.PARTITIONED);
        cCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cCfg.setSwapEnabled(false);
//        cCfg.setCopyOnRead(false);

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

        Ignition.start(createConfiguration("node", false));

        IgniteConfiguration clientCfg = createConfiguration("client", false);
        clientCfg.setClientMode(true);

        node = Ignition.start(clientCfg);

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

        Ignite server = Ignition.start(createConfiguration("node", true));

        IgniteConfiguration clientCfg = createConfiguration("client", true);
        clientCfg.setClientMode(true);

        node = Ignition.start(clientCfg);

        cache = node.cache("query");

        for (int i = 0; i < ROWS_COUNT; i++) {
            PersonSimple person = new PersonSimple(i, "Name ", "Surname ", (i + 1) * 100);
            cache.put(i, person);
        }
    }


    @GenerateMicroBenchmark
    public void testGet() throws Exception {
        int key  = ThreadLocalRandom.current().nextInt(0, ROWS_COUNT);

        PersonSimple personSimple = cache.get(key);

        if (personSimple == null)
            throw new IgniteException("Person is null");
    }

    //@GenerateMicroBenchmark
    public void testPut() throws Exception {
        cache.put(putCounter, new PersonSimple(putCounter, "Name ", "Surname ", (putCounter + 1) * 100));
        putCounter++;
   }

    //@GenerateMicroBenchmark
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
//        CliendServerNodeBenchmark benchmark = new CliendServerNodeBenchmark();
//
//        benchmark.startNewNodes();
//
//        for (int i = ROWS_COUNT + 1; i < 1000000; i++) {
//            benchmark.cache.put(i, new PersonSimple(i, "Name  DSda hdasjhdkas ajksdhasjkd " + i, "Surname dasdsad " + i,
//                                                    (i + 1) * 100));
//
//            if (i % 10000 == 0)
//                System.out.println("Put :" + i);
//        }
//
//        System.out.println("Put all the data!");
//    }


    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
            .include(".*" + CliendServerNodeBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(15)
            .measurementIterations(65)
            .jvmArgs("-server")
            .forks(1)
            .outputFormat(OutputFormatType.TextReport)
                //.shouldDoGC(true)
            .build();

        new Runner(opts).run();
    }

}
