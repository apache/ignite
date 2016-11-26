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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Transaction finish response.
 */
public class GridDistributedTxFinishResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheVersion txId;

    /** Future ID. */
    private IgniteUuid futId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDistributedTxFinishResponse() {
        /* No-op. */
    }

    /**
     * @param txId Transaction id.
     * @param futId Future ID.
     */
    public GridDistributedTxFinishResponse(GridCacheVersion txId, IgniteUuid futId) {
        assert txId != null;
        assert futId != null;

        this.txId = txId;
        this.futId = futId;
    }

    /**
     *
     * @return Transaction id.
     */
    public GridCacheVersion xid() {
        return txId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.txFinishMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        /*
        TODO possible starvation - need to fix sync waits in internal threads.
        Thread [name="sys-stripe-1-#30%dht.GridCacheColocatedTxExceptionSelfTest1%", id=46, state=WAITING, blockCnt=0, waitCnt=7]
    Lock [object=o.a.i.i.processors.cache.distributed.dht.GridDhtTxFinishFuture@212874c9, ownerName=null, ownerId=-1]
        at sun.misc.Unsafe.park(Native Method)
        at java.util.concurrent.locks.LockSupport.park(LockSupport.java:186)
        at java.util.concurrent.locks.AbstractQueuedSynchronizer.parkAndCheckInterrupt(AbstractQueuedSynchronizer.java:834)
        at java.util.concurrent.locks.AbstractQueuedSynchronizer.doAcquireSharedInterruptibly(AbstractQueuedSynchronizer.java:994)
        at java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireSharedInterruptibly(AbstractQueuedSynchronizer.java:1303)
        at o.a.i.i.util.future.GridFutureAdapter.get0(GridFutureAdapter.java:159)
        at o.a.i.i.util.future.GridFutureAdapter.get(GridFutureAdapter.java:117)
        at o.a.i.i.processors.cache.distributed.dht.GridDhtTxFinishFuture.onError(GridDhtTxFinishFuture.java:183)
        at o.a.i.i.processors.cache.distributed.dht.GridDhtTxLocal.finishCommit(GridDhtTxLocal.java:543)
        at o.a.i.i.processors.cache.distributed.dht.GridDhtTxLocal.commitAsync(GridDhtTxLocal.java:580)
        at o.a.i.i.processors.cache.transactions.IgniteTxHandler.finishDhtLocal(IgniteTxHandler.java:849)
        at o.a.i.i.processors.cache.transactions.IgniteTxHandler.finish(IgniteTxHandler.java:728)
        at o.a.i.i.processors.cache.transactions.IgniteTxHandler.processNearTxFinishRequest(IgniteTxHandler.java:687)
        at o.a.i.i.processors.cache.transactions.IgniteTxHandler$3.apply(IgniteTxHandler.java:157)
        at o.a.i.i.processors.cache.transactions.IgniteTxHandler$3.apply(IgniteTxHandler.java:155)
        at o.a.i.i.processors.cache.GridCacheIoManager.processMessage(GridCacheIoManager.java:758)
        at o.a.i.i.processors.cache.GridCacheIoManager.onMessage0(GridCacheIoManager.java:363)
        at o.a.i.i.processors.cache.GridCacheIoManager.handleMessage(GridCacheIoManager.java:287)
        at o.a.i.i.processors.cache.GridCacheIoManager.access$000(GridCacheIoManager.java:89)
        at o.a.i.i.processors.cache.GridCacheIoManager$1.onMessage(GridCacheIoManager.java:232)
        at o.a.i.i.managers.communication.GridIoManager.invokeListener(GridIoManager.java:1212)
        at o.a.i.i.managers.communication.GridIoManager.processRegularMessage0(GridIoManager.java:840)
        at o.a.i.i.managers.communication.GridIoManager.access$2100(GridIoManager.java:110)
        at o.a.i.i.managers.communication.GridIoManager$6.run(GridIoManager.java:785)
        at o.a.i.i.util.StripedExecutor$Stripe.run(StripedExecutor.java:362)
        at java.lang.Thread.run(Thread.java:724)
         */
        return Integer.MIN_VALUE;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("txId", txId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                txId = reader.readMessage("txId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDistributedTxFinishResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishResponse.class, this);
    }
}
