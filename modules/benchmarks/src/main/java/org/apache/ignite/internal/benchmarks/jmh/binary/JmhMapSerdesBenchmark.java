/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jmh.binary;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/** */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Warmup(iterations = 3, time = 30, timeUnit = SECONDS)
@Measurement(iterations = 3, time = 30, timeUnit = SECONDS)
public class JmhMapSerdesBenchmark {
    /** */
    @Param({/*"Integer", "Date",*/ "String"/*, "Object"*/})
    private String key;

    /** */
    @Param({/*"Integer", "Date",*/ "String"/*, "Object"*/})
    private String value;

    /** */
    @Param({/* "10", "100", */ "1000"/*, "10000"*/})
    private String size;

    /** */
    @Param({"HashMap"/*, "ConcurrentHashMap", "LinkedHashMap"*/})
    private String mapType;

    /** */
    private BinaryContext bctx;

    /** */
    private BinaryOutputStream out;

    /** */
    private BinaryWriterEx writer;

    /** */
    private Map<Object, Object> data;

    /** */
    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(new String[]{JmhMapSerdesBenchmark.class.getName()});
    }

    /** */
    @Setup
    public void setup() throws Exception {
        IgniteEx node = (IgniteEx)Ignition.start(new IgniteConfiguration());

        bctx = node.context().cacheObjects().binaryContext();
        out = BinaryStreams.outputStream((int)(100 * U.MB));
        writer = BinaryUtils.writer(bctx, out);

        Ignition.stopAll(false);

        if ("HashMap".equals(mapType))
            data = new HashMap<>();
        else if ("ConcurrentHashMap".equals(mapType))
            data = new ConcurrentHashMap<>();
        else if ("LinkedHashMap".equals(mapType))
            data = new LinkedHashMap<>();
        else
            throw new IllegalArgumentException("Unknown map type: " + mapType);

        int sz = Integer.parseInt(size);
        for (int i = 0; i < sz; i++)
            data.put(produce(i, key), produce(i, value));
    }

    /** */
    public Object produce(int i, String type) {
        if ("Integer".equals(type))
            return i;
        else if ("Date".equals(type))
            return new Date(System.currentTimeMillis() + i);
        else if ("String".equals(type))
            return "" + i;
        else if ("Object".equals(type)) {
            return new Employee("Name" + i, 1000L * i, new Date());
        }

        throw new IllegalArgumentException("Unknown type: " + type);
    }

    /** */
    @Benchmark
    public void mapSerialization(Blackhole bh) {
        BinaryWriterEx writer = BinaryUtils.writer(bctx, out);

        writer.writeMap(data);

        out.position(0);

        bh.consume(writer);
    }

    /** */
    public static class Employee {
        /** */
        private String fio;

        /** */
        private long salary;

        /** */
        private Date created;

        /** */
        public Employee(String fio, long salary, Date created) {
            this.fio = fio;
            this.salary = salary;
            this.created = created;
        }

        /** */
        public Employee() {
        }

        /** */
        public Employee(String fio, long salary) {
            this.fio = fio;
            this.salary = salary;
        }

        /** */
        public String getFio() {
            return fio;
        }

        /** */
        public void setFio(String fio) {
            this.fio = fio;
        }

        /** */
        public long getSalary() {
            return salary;
        }

        /** */
        public void setSalary(long salary) {
            this.salary = salary;
        }

        /** */
        public Date getCreated() {
            return created;
        }

        /** */
        public void setCreated(Date created) {
            this.created = created;
        }
    }
}
