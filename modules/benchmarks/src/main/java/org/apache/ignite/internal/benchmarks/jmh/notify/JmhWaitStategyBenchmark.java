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

package org.apache.ignite.internal.benchmarks.jmh.notify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 *
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({/*Mode.AverageTime,*/ Mode.Throughput})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class JmhWaitStategyBenchmark extends JmhCacheAbstractBenchmark {
    /** */
    private static class RandomExpiryPolicy implements ExpiryPolicy {
        /** rate duration will decrease with */
        private final double rate;

        /** current duration. */
        private final AtomicLong duration = new AtomicLong(1_000_000_000);

        /** */
        RandomExpiryPolicy(double rate) {
            this.rate = rate;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            boolean generateEvt = ThreadLocalRandom.current().nextDouble() < rate;
            return generateEvt ? new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet()) :
                new Duration(TimeUnit.MILLISECONDS, duration.get());
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            boolean generateEvt = ThreadLocalRandom.current().nextDouble() < rate;
            return generateEvt ? new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet()) :
                new Duration(TimeUnit.MILLISECONDS, duration.get());
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            boolean generateEvt = ThreadLocalRandom.current().nextDouble() < rate;
            return generateEvt ? new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet()) :
                new Duration(TimeUnit.MILLISECONDS, duration.get());
        }
    }

    /** @param rate duration will decrease with */
    private static Factory<ExpiryPolicy> getExpiryPolicyFactoryWithDecreasingRate(final double rate) {
        return new Factory<ExpiryPolicy>() {
            @Override public ExpiryPolicy create() {
                return new RandomExpiryPolicy(rate);
            }
        };
    }

    /** Decreasing expiry policy. */
    private static final ExpiryPolicy DECREASING_EXPIRY_POLICY = new ExpiryPolicy() {
        AtomicLong duration = new AtomicLong(1_000_000_000);

        @Override public Duration getExpiryForCreation() {
            return new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet());
        }

        @Override public Duration getExpiryForAccess() {
            return new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet());
        }

        @Override public Duration getExpiryForUpdate() {
            return new Duration(TimeUnit.MILLISECONDS, duration.decrementAndGet());
        }
    };

    /** Increasing expiry policy. */
    private static final ExpiryPolicy INCREASING_EXPIRY_POLICY = new ExpiryPolicy() {
        AtomicLong duration = new AtomicLong(1_000_000);

        @Override public Duration getExpiryForCreation() {
            return new Duration(TimeUnit.MILLISECONDS, duration.incrementAndGet());
        }

        @Override public Duration getExpiryForAccess() {
            return new Duration(TimeUnit.MILLISECONDS, duration.incrementAndGet());
        }

        @Override public Duration getExpiryForUpdate() {
            return new Duration(TimeUnit.MILLISECONDS, duration.incrementAndGet());
        }
    };

    /** Decreasing policy factory. */
    private static final Factory<ExpiryPolicy> DECREASING_POLICY_FACTORY = new Factory<ExpiryPolicy>() {
        @Override public ExpiryPolicy create() {
            return DECREASING_EXPIRY_POLICY;
        }
    };

    /** Increasing policy factory. */
    private static final Factory<ExpiryPolicy> INCREASING_POLICY_FACTORY = new Factory<ExpiryPolicy>() {
        @Override public ExpiryPolicy create() {
            return INCREASING_EXPIRY_POLICY;
        }
    };

    /** {@inheritDoc} */
    @Setup (Level.Iteration)
    @Override public void setup() throws Exception {
        Ignition.stopAll(true);

        super.setup();

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
        cfg.setName("cache");
        cfg.setEagerTtl(true);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        String prop = System.getProperty("bench.exp.policy");

        switch (prop) {
            case "inc":
                cfg.setExpiryPolicyFactory(INCREASING_POLICY_FACTORY);
                break;
            case "dec":
                cfg.setExpiryPolicyFactory(DECREASING_POLICY_FACTORY);
                break;
            default:
                assert prop.charAt(0) == 'r';
                double rate = Double.parseDouble(prop.trim().substring(1)) / 100;
                cfg.setExpiryPolicyFactory(getExpiryPolicyFactoryWithDecreasingRate(rate));
                break;
        }

        node.createCache(cfg);

        cache = node.getOrCreateCache("cache");

        IgniteDataStreamer<Integer, IntValue> dataLdr = node.dataStreamer(cache.getName());

        for (int i = 0; i < CNT; i++)
            dataLdr.addData(i, new IntValue(i));

        dataLdr.close();

        System.out.println("Cache populated.");
    }

    /** {@inheritDoc} */
    @TearDown
    @Override public void tearDown() throws Exception {
        Ignition.stopAll(true);
    }

    /**
     * Test PUT operation.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public void put() throws Exception {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(key, new IntValue(key));
    }

    /**
     * Benchmark runner
     */
    public static void main(String[] args) throws RunnerException {
        List<String> policies = Arrays.asList("inc", "dec", "r25", "r50", "r75");
        int[] threads = {2, 4, 8, 16, 32};

        List<RunResult> results = new ArrayList<>();

        for (String policy : policies) {
            for (int thread : threads) {
                ChainedOptionsBuilder builder = new OptionsBuilder()
                    .jvmArgs()
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .measurementIterations(10)
                    .measurementTime(TimeValue.seconds(20))
                    .warmupIterations(5)
                    .warmupTime(TimeValue.seconds(10))
                    .jvmArgs("-Dbench.exp.policy=" + policy)
                    .forks(1)
                    .threads(thread)
                    .mode(Mode.Throughput)
                    .include(JmhWaitStategyBenchmark.class.getSimpleName());

                results.addAll(new Runner(builder.build()).run());
            }
        }

        for (RunResult result : results) {
            BenchmarkParams params = result.getParams();
            Collection<String> args1 = params.getJvmArgs();
            for (String s : args1) {
                System.out.print(s.substring(s.length() - 3, s.length()));
                System.out.print(" x ");
            }
            System.out.print(params.getThreads());
            System.out.print("\t\t");
            System.out.println(result.getPrimaryResult().toString());
        }
    }
}
