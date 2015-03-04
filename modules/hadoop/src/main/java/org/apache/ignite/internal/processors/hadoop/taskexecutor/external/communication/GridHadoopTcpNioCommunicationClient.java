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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.hadoop.message.*;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Grid client for NIO server.
 */
public class GridHadoopTcpNioCommunicationClient extends GridHadoopAbstractCommunicationClient {
    /** Socket. */
    private final GridNioSession ses;

    /**
     * Constructor for test purposes only.
     */
    public GridHadoopTcpNioCommunicationClient() {
        ses = null;
    }

    /**
     * @param ses Session.
     */
    public GridHadoopTcpNioCommunicationClient(GridNioSession ses) {
        assert ses != null;

        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res)
            ses.close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(GridHadoopProcessDescriptor desc, GridHadoopMessage msg)
        throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(msg);

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to send message [client=" + this + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        long now = U.currentTimeMillis();

        // Session can be used for receiving and sending.
        return Math.min(Math.min(now - ses.lastReceiveTime(), now - ses.lastSendScheduleTime()),
            now - ses.lastSendTime());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTcpNioCommunicationClient.class, this, super.toString());
    }
}
