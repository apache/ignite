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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiNoop;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 * No-operation SPI for standalone WAL reader
 */
@IgniteSpiNoop
public class StandaloneNoopCommunicationSpi extends IgniteSpiAdapter implements CommunicationSpi {
    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode destNode, Serializable msg) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {

    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable CommunicationListener lsnr) {

    }
}
