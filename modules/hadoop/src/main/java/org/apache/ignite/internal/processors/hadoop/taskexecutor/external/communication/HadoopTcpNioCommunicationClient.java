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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessDescriptor;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Grid client for NIO server.
 */
public class HadoopTcpNioCommunicationClient extends HadoopAbstractCommunicationClient {
    /** Socket. */
    private final GridNioSession ses;

    /**
     * Constructor for test purposes only.
     */
    public HadoopTcpNioCommunicationClient() {
        ses = null;
    }

    /**
     * @param ses Session.
     */
    public HadoopTcpNioCommunicationClient(GridNioSession ses) {
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
    @Override public void sendMessage(HadoopProcessDescriptor desc, HadoopMessage msg)
        throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(msg);

        if (fut.isDone())
            fut.get();
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
        return S.toString(HadoopTcpNioCommunicationClient.class, this, super.toString());
    }
}