/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh;

import org.apache.ignite.jmh.model.*;
import org.apache.ignite.marshaller.optimized.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.logic.results.*;
import org.openjdk.jmh.output.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
@State
public class ReadFieldBenchmark {

    private OptimizedMarshaller marshaller;

    private byte[] arr;

    @Setup
    public void init() throws Exception {
        marshaller = new OptimizedMarshaller();

        marshaller.setContext(new MarshallerContextJMHImpl());

        /*marshaller.setMetadataHandler(new OptimizedMarshallerMetaHandler() {

            private ConcurrentHashMap<Integer, OptimizedObjectMetadata> map = new ConcurrentHashMap<Integer,
                OptimizedObjectMetadata>();

            @Override public void addMeta(int typeId, OptimizedObjectMetadata meta) {
                map.put(typeId, meta);
            }

            @Override public OptimizedObjectMetadata metadata(int typeId) {
                return map.get(typeId);
            }
        });

        marshaller.enableFieldsIndexing(PersonSimple.class);
*/
        PersonSimple person = new PersonSimple(100, "Denis A", "Magda", 200);

        arr = marshaller.marshal(person);
    }

    @GenerateMicroBenchmark
    public void testReadField() throws Exception {
        //boolean res = marshaller.hasField("firstName", arr, 0, arr.length);

        //if (!res)
        //    throw new RuntimeException("Fuck");

        Object name = marshaller.readField("id", arr, 0, arr.length, null);

        if (name == null)
            throw new RuntimeException("Fuck");
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
            .include(".*")
            .warmupIterations(15)
            .measurementIterations(15)
            .jvmArgs("-server")
            .forks(1)
            .outputFormat(OutputFormatType.TextReport)
            .build();

        Map<BenchmarkRecord,RunResult> records = new Runner(opts).run();
        /*for (Map.Entry<BenchmarkRecord, RunResult> result : records.entrySet()) {
            Result r = result.getValue().getPrimaryResult();
            System.out.println("API replied benchmark score: "
                                   + r.getScore() + " "
                                   + r.getScoreUnit() + " over "
                                   + r.getStatistics().getN() + " iterations");
        }*/
    }
}
