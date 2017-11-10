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

package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
public class GridNioCompressFilter extends GridNioFilterAdapter {
    /** */
    private static final int COMPRESS_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private IgniteLogger log;

    /** Whether direct mode is used. */
    private boolean directMode;

    /** */
    public GridNioCompressFilter(IgniteLogger log, boolean directMode) {
        super("Compress filter");

        this.log = log;
        this.directMode = directMode;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override
    public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        if (msg instanceof ByteBuffer)
            System.out.println("MY !!!! ="+((ByteBuffer)msg).limit());
        if (directMode)
            return proceedSessionWrite(ses, msg, fut, ackC);

        ByteBuffer input = checkMessage(ses, msg);

        if (!input.hasRemaining())
            return new GridNioFinishedFuture<Object>(null);

        try {
            return proceedSessionWrite(ses, new GridNioCompressor().compress(input), fut, ackC);
        }
        catch (IOException e) {
            throw new GridNioException("Failed to compress data: " + ses, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        ByteBuffer input = checkMessage(ses, msg);
        try {
            Object meta = ses.meta(COMPRESS_META_KEY);
            if (meta != null) {
                System.out.println("\t\thave meta");
                ByteOrder order = input.order();
                ByteBuffer buf2;
                if (input.isDirect()) {
                    buf2 = ByteBuffer.allocateDirect(((ByteBuffer)meta).limit()+input.limit());
                } else {
                    buf2 = ByteBuffer.allocateDirect(((ByteBuffer)meta).limit()+input.limit());
                }
                buf2.order(order);
                buf2.put(((ByteBuffer)meta));
                buf2.put(input);
                ses.removeMeta(COMPRESS_META_KEY);
                buf2.flip();
                buf2 = new GridNioCompressor().decompress(buf2);

                proceedMessageReceived(ses, buf2);
                return;
            }
            input = new GridNioCompressor().decompress(input);

            proceedMessageReceived(ses, input);
        }
        catch (IOException e) {
            //throw new GridNioException("Failed to decompress data: " + ses, e);
            input.rewind();
            ses.addMeta(COMPRESS_META_KEY, input);
            System.out.println("\t\tFailed to decompress data: " + input.limit());
            //e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * @return Flag indicating whether direct mode is used.
     */
    public boolean directMode() {
        return directMode;
    }

    /**
     * Checks type of the message passed to the filter and converts it to a byte buffer (since compress filter
     * operates only on binary data).
     *
     * @param ses Session instance.
     * @param msg Message passed in.
     * @return Message that was cast to a byte buffer.
     * @throws GridNioException If msg is not a byte buffer.
     */
    private ByteBuffer checkMessage(GridNioSession ses, Object msg) throws GridNioException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Invalid object type received (is compress filter correctly placed in filter " +
                "chain?) [ses=" + ses + ", msgClass=" + msg.getClass().getName() +  ']');

        return (ByteBuffer)msg;
    }
}
