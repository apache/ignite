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

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.junit.Assert;

/**
 * Top-level closure class.
 */
public class TestClosure implements IgniteClosure<Object, Object> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @LoggerResource
    private IgniteLogger log;

    @Override public Object apply(Object o) {
        Assert.assertNotNull(ignite);
        Assert.assertNotNull(log);

        log.info("Closure is running with grid: " + ignite);

        return null;
    }
}