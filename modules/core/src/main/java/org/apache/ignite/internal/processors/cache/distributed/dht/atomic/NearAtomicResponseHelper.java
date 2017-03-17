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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 *
 */
public class NearAtomicResponseHelper {

    /** */
    private GridNearAtomicUpdateResponse res;

    private static final AtomicIntegerFieldUpdater<NearAtomicResponseHelper> UPD =
        AtomicIntegerFieldUpdater.newUpdater(NearAtomicResponseHelper.class, "cnt");

    /** */
    private volatile int cnt;

    /**
     */
    public NearAtomicResponseHelper(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @param res Response.
     * @return {@code true} if all responses added.
     */
    public GridNearAtomicUpdateResponse addResponse(GridNearAtomicUpdateResponse res) {
        this.res = res;

        int c = UPD.decrementAndGet(this);

        //mergeResponse(res);

        if (c == 0)
            return this.res;

        return null;
    }

    /**
     * @param res Response.
     */
    private void mergeResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null)
            this.res = res;
        else {
            if (res.nearValuesIndexes() != null)
                for (int i = 0; i < res.nearValuesIndexes().size(); i++)
                    this.res.addNearValue(
                        res.nearValuesIndexes().get(i),
                        res.nearValue(i),
                        res.nearTtl(i),
                        res.nearExpireTime(i)
                    );

            if (res.failedKeys() != null)
                this.res.addFailedKeys(res.failedKeys(), null);

            if (res.skippedIndexes() != null)
                this.res.skippedIndexes().addAll(res.skippedIndexes());
        }
    }
}
