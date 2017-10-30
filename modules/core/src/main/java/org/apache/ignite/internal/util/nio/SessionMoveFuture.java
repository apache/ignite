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

import java.nio.channels.SocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class SessionMoveFuture extends NioOperationFuture<Boolean> {
    /** */
    private final int toIdx;

    /** */
    @GridToStringExclude
    private SocketChannel movedSockCh;

    /**
     * @param ses Session.
     * @param toIdx Target worker index.
     */
    SessionMoveFuture(
        GridSelectorNioSessionImpl ses,
        int toIdx
    ) {
        super(ses, NioOperation.MOVE);

        this.toIdx = toIdx;
    }

    /**
     * @return Target worker index.
     */
    int toIndex() {
        return toIdx;
    }

    /**
     * @return Moved session socket channel.
     */
    SocketChannel movedSocketChannel() {
        return movedSockCh;
    }

    /**
     * @param movedSockCh Moved session socket channel.
     */
    void movedSocketChannel(SocketChannel movedSockCh) {
        assert movedSockCh != null;

        this.movedSockCh = movedSockCh;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionMoveFuture.class, this, super.toString());
    }
}
