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

package org.apache.ignite.yardstick.upload;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.upload.model.Values10;

/**
 * Benchmark that inserts single upload of number of entries using {@link IgniteCache#put(Object, Object)}.
 */
public class NativePutBenchmark extends AbstractNativeBenchmark {
    /**
     * Uploads randomly generated data using simple put.
     *
     * @param cacheName - name of the cache.
     * @param insertsCnt - how many entries should be uploaded.
     */
    @Override protected void upload(String cacheName, long insertsCnt) {
        IgniteCache<Object, Object> c = ignite().cache(cacheName);

        for (long id = 1; id <= insertsCnt; id++)
            c.put(id, new Values10() );
    }
}
