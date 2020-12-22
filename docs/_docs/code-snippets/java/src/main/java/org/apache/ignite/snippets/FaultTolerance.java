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
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.failover.never.NeverFailoverSpi;

public class FaultTolerance {
    void always() {
        // tag::always[]
        AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();

        // Override maximum failover attempts.
        failSpi.setMaximumFailoverAttempts(5);

        // Override the default failover SPI.
        IgniteConfiguration cfg = new IgniteConfiguration().setFailoverSpi(failSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::always[]

        ignite.close();
    }

    void never() {
        // tag::never[]
        NeverFailoverSpi failSpi = new NeverFailoverSpi();

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override the default failover SPI.
        cfg.setFailoverSpi(failSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::never[]

        ignite.close();
    }

    public static void main(String[] args) {
        FaultTolerance ft = new FaultTolerance();

        ft.always();
        ft.never();
    }
}
