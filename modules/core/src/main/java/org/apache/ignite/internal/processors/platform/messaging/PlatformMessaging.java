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

package org.apache.ignite.internal.processors.platform.messaging;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;

import java.util.UUID;

/**
 * Interop messaging.
 */
public class PlatformMessaging extends PlatformAbstractTarget {
    /** */
    public static final int OP_LOC_LISTEN = 1;

    /** */
    public static final int OP_REMOTE_LISTEN = 2;

    /** */
    public static final int OP_SEND = 3;

    /** */
    public static final int OP_SEND_MULTI = 4;

    /** */
    public static final int OP_SEND_ORDERED = 5;

    /** */
    public static final int OP_STOP_LOC_LISTEN = 6;

    /** */
    public static final int OP_STOP_REMOTE_LISTEN = 7;

    /** */
    public static final int OP_WITH_ASYNC = 8;

    /** */
    public static final int OP_REMOTE_LISTEN_ASYNC = 9;

    /** */
    public static final int OP_STOP_REMOTE_LISTEN_ASYNC = 10;

    /** */
    private final IgniteMessaging messaging;

    /** */
    private final IgniteMessaging messagingAsync;

    /**
     * Ctor.
     *
     * @param platformCtx Context.
     * @param messaging Ignite messaging.
     */
    public PlatformMessaging(PlatformContext platformCtx, IgniteMessaging messaging) {
        super(platformCtx);

        assert messaging != null;

        this.messaging = messaging;
        messagingAsync = messaging.withAsync();
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_SEND:
                messaging.send(reader.readObjectDetached(), reader.readObjectDetached());

                return TRUE;

            case OP_SEND_MULTI:
                messaging.send(reader.readObjectDetached(), PlatformUtils.readCollection(reader));

                return TRUE;

            case OP_SEND_ORDERED:
                messaging.sendOrdered(reader.readObjectDetached(), reader.readObjectDetached(), reader.readLong());

                return TRUE;

            case OP_LOC_LISTEN: {
                PlatformMessageLocalFilter filter = new PlatformMessageLocalFilter(reader.readLong(), platformCtx);

                Object topic = reader.readObjectDetached();

                messaging.localListen(topic, filter);

                return TRUE;
            }

            case OP_STOP_LOC_LISTEN: {
                PlatformMessageLocalFilter filter = new PlatformMessageLocalFilter(reader.readLong(), platformCtx);

                Object topic = reader.readObjectDetached();

                messaging.stopLocalListen(topic, filter);

                return TRUE;
            }

            case OP_STOP_REMOTE_LISTEN: {
                messaging.stopRemoteListen(reader.readUuid());

                return TRUE;
            }

            case OP_REMOTE_LISTEN_ASYNC: {
                startRemoteListen(reader, messagingAsync);

                return readAndListenFuture(reader);
            }

            case OP_STOP_REMOTE_LISTEN_ASYNC: {
                messagingAsync.stopRemoteListen(reader.readUuid());

                return readAndListenFuture(reader);
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "ConstantConditions", "unchecked"})
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_REMOTE_LISTEN:{
                writer.writeUuid(startRemoteListen(reader, messaging));

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /**
     * Starts the remote listener.
     * @param reader Reader.
     * @return Listen id.
     */
    private UUID startRemoteListen(BinaryRawReaderEx reader, IgniteMessaging messaging) {
        Object nativeFilter = reader.readObjectDetached();

        long ptr = reader.readLong();  // interop pointer

        Object topic = reader.readObjectDetached();

        PlatformMessageFilter filter = platformCtx.createRemoteMessageFilter(nativeFilter, ptr);

        return messaging.remoteListen(topic, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        return ((IgniteFutureImpl)messagingAsync.future()).internalFuture();
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_ASYNC:
                if (messaging.isAsync())
                    return this;

                return new PlatformMessaging (platformCtx, messaging.withAsync());
        }

        return super.processOutObject(type);
    }
}
