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

package org.apache.ignite.yardstick.cache.store.jdbc;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.SampleKey;

/**
 * Ignite JDBC cache store benchmark that performs get operations.
 */
public class IgniteJdbcStoreGetBenchmark extends IgniteJdbcStoreAbstractBenchmark {
    /** {@inheritDoc} */
    @Override protected int fillRange() {
        return args.range();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int id = nextRandom(args.range());

        cache().get(new SampleKey(id));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache() {
        return ignite().cache("atomic");
    }
}
