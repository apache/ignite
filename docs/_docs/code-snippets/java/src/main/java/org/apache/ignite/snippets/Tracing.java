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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.transactions.Transaction;
import org.junit.jupiter.api.Test;

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;

public class Tracing {

    @Test
    void config() {
        //tag::config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setTracingSpi(new org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi());

        Ignite ignite = Ignition.start(cfg);
        //end::config[]

        ignite.close();
    }

    @Test
    void enableSampling() {
        //tag::enable-sampling[]
        Ignite ignite = Ignition.start();

        ignite.tracingConfiguration().set(
                new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
                new TracingConfigurationParameters.Builder().withSamplingRate(1).build());

        //end::enable-sampling[]
        ignite.close();
    }

    void exportToZipkin() {
        //tag::export-to-zipkin[]
        //register Zipkin exporter
        ZipkinTraceExporter.createAndRegister(
                ZipkinExporterConfiguration.builder().setV2Url("http://localhost:9411/api/v2/spans")
                        .setServiceName("ignite-cluster").build());

        IgniteConfiguration cfg = new IgniteConfiguration().setClientMode(true)
                .setTracingSpi(new org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi());

        Ignite ignite = Ignition.start(cfg);

        //enable trace sampling for transactions with 100% sampling rate
        ignite.tracingConfiguration().set(
                new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
                new TracingConfigurationParameters.Builder().withSamplingRate(1).build());

        //create a transactional cache
        IgniteCache<Integer, String> cache = ignite
                .getOrCreateCache(new CacheConfiguration<Integer, String>("myCache")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        IgniteTransactions transactions = ignite.transactions();

        // start a transaction
        try (Transaction tx = transactions.txStart()) {
            //do some operations
            cache.put(1, "test value");

            System.out.println(cache.get(1));

            cache.put(1, "second value");

            tx.commit();
        }

        try {
            //This code here is to wait until the trace is exported to Zipkin. 
            //If your application doesn't stop here, you don't need this piece of code. 
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        //end::export-to-zipkin[]
        ignite.close();
    }
}
