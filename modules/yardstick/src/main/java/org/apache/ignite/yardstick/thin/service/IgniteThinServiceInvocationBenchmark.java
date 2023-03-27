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

package org.apache.ignite.yardstick.thin.service;

import java.util.Map;
import org.apache.ignite.yardstick.IgniteThinAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Class to benchmark thin client service invocation.
 */
public class IgniteThinServiceInvocationBenchmark extends IgniteThinAbstractBenchmark {
    /** Service proxy. */
    private volatile ThreadLocal<SimpleService> srvcProxy;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        srvcProxy = ThreadLocal.withInitial(
            () -> client().services().serviceProxy(SimpleService.class.getSimpleName(), SimpleService.class));
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        srvcProxy.get().echo(nextRandom(Integer.MAX_VALUE));

        return true;
    }
}
