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

package org.apache.ignite.tests.load.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.tests.load.Worker;

/**
 * Ignite load tests worker for read operation CacheStore.load
 */
public class ReadWorker extends Worker {
    /** */
    public static final String LOGGER_NAME = "IgniteReadLoadTest";

    /** */
    public ReadWorker(Ignite ignite, long startPosition, long endPosition) {
        super(ignite, startPosition, endPosition);
    }

    /** {@inheritDoc} */
    @Override protected String loggerName() {
        return LOGGER_NAME;
    }

    /** {@inheritDoc} */
    @Override protected boolean batchMode() {
        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void process(IgniteCache cache, Object key, Object val) {
        cache.get(key);
    }
}
