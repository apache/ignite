/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.messaging;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteFuture;

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
                readAndListenFuture(reader, startRemoteListenAsync(reader, messaging));

                return TRUE;
            }

            case OP_STOP_REMOTE_LISTEN_ASYNC: {
                readAndListenFuture(reader, messaging.stopRemoteListenAsync(reader.readUuid()));

                return TRUE;
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
     * @param messaging Messaging.
     * @return Listen id.
     */
    private UUID startRemoteListen(BinaryRawReaderEx reader, IgniteMessaging messaging) {
        Object nativeFilter = reader.readObjectDetached();

        long ptr = reader.readLong();  // interop pointer

        Object topic = reader.readObjectDetached();

        PlatformMessageFilter filter = platformCtx.createRemoteMessageFilter(nativeFilter, ptr);

        return messaging.remoteListen(topic, filter);
    }

    /**
     * Starts the remote listener.
     * @param reader Reader.
     * @param messaging Messaging.
     * @return Future of the operation.
     */
    private IgniteFuture<UUID> startRemoteListenAsync(BinaryRawReaderEx reader, IgniteMessaging messaging) {
        Object nativeFilter = reader.readObjectDetached();

        long ptr = reader.readLong();  // interop pointer

        Object topic = reader.readObjectDetached();

        PlatformMessageFilter filter = platformCtx.createRemoteMessageFilter(nativeFilter, ptr);

        return messaging.remoteListenAsync(topic, filter);
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
