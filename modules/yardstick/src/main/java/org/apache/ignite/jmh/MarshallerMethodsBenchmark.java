/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh;

import org.apache.ignite.jmh.model.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.output.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import java.util.concurrent.*;

/**
 * Created by GridAdmin1234 on 6/30/2015.
 */
@State(value = Scope.Benchmark)
public class MarshallerMethodsBenchmark {
    private OptimizedMarshaller marsh;

    private PersonSimple personSimple;

    @Setup
    public void initNewMarshaller() {
        marsh = new OptimizedMarshaller(false);
        marsh.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1_1);

        marsh.setContext(new MarshallerContextJMHImpl());

        OptimizedMarshallerIndexingHandler idxHandler = new OptimizedMarshallerIndexingHandler();

        idxHandler.setMetaHandler(new OptimizedMarshallerMetaHandler() {

            private ConcurrentHashMap<Integer, OptimizedObjectMetadata> map = new ConcurrentHashMap<Integer,
                OptimizedObjectMetadata>();

            @Override public void addMeta(int typeId, OptimizedObjectMetadata meta) {
                map.put(typeId, meta);
            }

            @Override public OptimizedObjectMetadata metadata(int typeId) {
                return map.get(typeId);
            }
        });

        marsh.setIndexingHandler(idxHandler);

        personSimple = new PersonSimple(100, 200, "asdjkas aksjdhkajsd ajd", "asjdlaskd askdjsalkd", 900);
    }

    //@Setup
    public void initOldMarshaller() {
        marsh = new OptimizedMarshaller(false);

        marsh.setProtocolVersion(OptimizedMarshallerProtocolVersion.VER_1);
        marsh.setContext(new MarshallerContextJMHImpl());

        personSimple = new PersonSimple(100, 200, "asdjkas aksjdhkajsd ajd", "asjdlaskd askdjsalkd", 900);
    }

    @GenerateMicroBenchmark
    public void testMarshal() throws Exception {
        marsh.marshal(personSimple);
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
            .include(".*" + MarshallerMethodsBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(15)
            .measurementIterations(65)
            .jvmArgs("-server")
            .forks(1)
            .outputFormat(OutputFormatType.TextReport)
                //.shouldDoGC(true)
            .build();

        new Runner(opts).run();
    }

//    public static void main(String[] args) throws Exception {
//        MarshallerMethodsBenchmark benchmark = new MarshallerMethodsBenchmark();
//
//        benchmark.initNewMarshaller();
//
//        for (int i = 0; i < 40; i++)
//            benchmark.testMarshal();
//    }
}
