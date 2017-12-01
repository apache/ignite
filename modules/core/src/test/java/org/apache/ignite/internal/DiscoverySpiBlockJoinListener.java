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

package org.apache.ignite.internal;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class DiscoverySpiBlockJoinListener implements IgniteDiscoverySpiInternalListener {
    /** */
    private volatile CountDownLatch writeLatch;

    /**
     *
     */
    public void startBlock() {
        writeLatch = new CountDownLatch(1);
    }

    /**
     *
     */
    public void stopBlock() {
        writeLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void beforeJoin(IgniteLogger log) {
        try {
            CountDownLatch writeLatch0 = writeLatch;

            if (writeLatch0 != null) {
                log.info("Block join");

                U.await(writeLatch0);
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
