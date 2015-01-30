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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.internal.util.direct.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Parser for direct messages.
 */
public class GridDirectParser implements GridNioParser {
    /** Message metadata key. */
    private static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private IgniteSpiAdapter spi;

    /** */
    private MessageFactory msgFactory;

    /**
     * @param spi Spi.
     */
    public GridDirectParser(IgniteSpiAdapter spi) {
        this.spi = spi;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        if (msgFactory == null)
            msgFactory = spi.getSpiContext().messageFactory();

        GridTcpCommunicationMessageAdapter msg = ses.removeMeta(MSG_META_KEY);

        if (msg == null && buf.hasRemaining())
            msg = msgFactory.create(buf.get());

        boolean finished = false;

        if (buf.hasRemaining())
            finished = msg.readFrom(buf);

        if (finished)
            return msg;
        else {
            ses.addMeta(MSG_META_KEY, msg);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        // No encoding needed for direct messages.
        throw new UnsupportedEncodingException();
    }
}
