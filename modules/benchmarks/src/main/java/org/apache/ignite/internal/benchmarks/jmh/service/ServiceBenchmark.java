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

package org.apache.ignite.internal.benchmarks.jmh.service;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** */
@State(Scope.Benchmark)
public class ServiceBenchmark extends JmhAbstractBenchmark implements InvocationHandler {
    /** */
    private IgniteEx ignite;

    /** */
    private TestServiceImpl loc;

    /** */
    private TestService proxy;

    /** */
    private TestService test;

    /** */
    @Benchmark
    public void directReference(Blackhole blackhole) throws Exception {
        blackhole.consume(loc.handleVal(5));
    }

    /** */
    @Benchmark
    public void testProxy(Blackhole blackhole) throws Exception {
        blackhole.consume(test.handleVal(5));
    }

    /** */
    @Benchmark
    public void serviceProxy(Blackhole blackhole) throws Exception {
        blackhole.consume(proxy.handleVal(5));
    }

    /** */
    @Setup
    public void setup() throws Exception {
        ignite = (IgniteEx)Ignition.start(configuration("grid0"));

        ignite.services().deploy(ignite.services().nodeSingletonConfiguration("srv", new TestServiceImpl()));

        loc = ignite.services().service("srv");

        test = (TestService)Proxy.newProxyInstance(loc.getClass().getClassLoader(), new Class<?>[] {TestService.class}, this);

        proxy = new GridServiceProxy<>(ignite.cluster(), "srv", TestService.class, true, 0, ignite.context()).proxy();
//        proxy = ignite.services().serviceProxy("srv", TestService.class, true);
    }

    /** */
    @TearDown
    public void shutdown() {
        Ignition.stopAll(true);
    }

    /** */
    protected IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** */
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(ServiceBenchmark.class.getSimpleName())
            .threads(1)
            .forks(1)
            .warmupIterations(30)
            .measurementIterations(10)
            .jvmArgs("-Xms1g", "-Xmx1g")
            .build();

        new Runner(opt).run();
    }

    /** */
    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(loc, args);
    }

    /** */
    protected interface TestService {
        /** */
        default int handleVal(int val) {
            return val;
        }
    }

    /** */
    protected static class TestServiceImpl implements Service, TestService {
        /** */
        @Override public void cancel(ServiceContext ctx) {
        }

        /** */
        @Override public void init(ServiceContext ctx) throws Exception {
        }

        /** */
        @Override public void execute(ServiceContext ctx) throws Exception {
        }
    }
}
