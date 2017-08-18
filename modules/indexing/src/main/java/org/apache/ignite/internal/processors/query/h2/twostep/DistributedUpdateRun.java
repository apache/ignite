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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;

/** */
class DistributedUpdateRun {
    /** */
    private CountDownLatch latch;

    /** */
    private AtomicLong updCntr = new AtomicLong();

    /** */
    private volatile IgniteException ex;

    /** */
    DistributedUpdateRun(int nodeCount) {
        latch = new CountDownLatch(nodeCount);
    }

    /**
     * @return Latch.
     */
    CountDownLatch latch() {
        return latch;
    }

    /** */
    void handleNodeLeft(UUID nodeId) {
        ex = new IgniteException("Update failed: node " + nodeId + " has left");

        bringDownLatch();
    }

    /** */
    public boolean handleResponse(UUID id, GridH2DmlResponse msg) {
        if (msg.status() == GridH2DmlResponse.STATUS_ERROR) {
            ex = new IgniteException(msg.error());

            bringDownLatch();

            return true;
        }
        //TODO: handle errKeys

        this.updCntr.addAndGet(msg.updateCounter());

        latch.countDown();

        return latch.getCount() == 0L;
    }

    private void bringDownLatch() {
        while (latch.getCount() > 0)
            latch.countDown();
    }

    /** */
    public long updateCounter() throws IgniteException {

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }
        //TODO: while timeout await/ while (!U.await(r.latch(), 500, TimeUnit.MILLISECONDS)) {
        //TODO: throw error
        //TODO: cancelation
        if (ex != null)
            throw ex;

        return updCntr.get();
    }

    /** */
    public void handleDisconnect(CacheException e) {
        ex = new IgniteException(e);

        bringDownLatch();
    }
}
