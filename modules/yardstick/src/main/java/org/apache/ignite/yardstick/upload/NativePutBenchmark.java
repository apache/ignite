/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * Benchmark performs single upload of number of entries using {@link IgniteCache#put(Object, Object)}.
 */
public class NativePutBenchmark extends AbstractNativeBenchmark {
    /**
     * Uploads randomly generated data using simple put.
     *
     * @param insertsCnt - how many entries should be uploaded.
     */
    @Override protected void upload(long insertsCnt) {
        IgniteCache<Object, Object> c = ignite().cache(CACHE_NAME);

        for (long id = 1; id <= insertsCnt; id++)
            c.put(id, new Values10());
    }
}
