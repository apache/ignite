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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.upload.model.Values10;

/**
 * Upload benchmark that uses native sql INSERT operation for data uploading.
 */
public class NativeSqlInsertBenchmark extends AbstractNativeBenchmark {
    /**
     * Performs data upload using native sql and SqlFieldsQuery.
     *
     * @param insertsCnt how many rows to upload.
     */
    @Override protected void upload(long insertsCnt) {
        IgniteCache<Object, Object> c = ignite().cache(CACHE_NAME);

        SqlFieldsQuery ins = new SqlFieldsQuery(queries.insert());

        for (long i = 1; i <= insertsCnt; i++) {
            Object[] args = new Values10().toArgs(i);

            c.query(ins.setArgs(args)).getAll();
        }
    }
}
