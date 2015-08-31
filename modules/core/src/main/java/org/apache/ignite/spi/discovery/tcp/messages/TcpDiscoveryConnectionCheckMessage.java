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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

/**
 * Message used to check whether a node is still connected to the topology.
 * The difference from {@link TcpDiscoveryStatusCheckMessage} is that this message is sent to the next node
 * which directly replies to the sender without message re-translation to the coordinator.
 */
public class TcpDiscoveryConnectionCheckMessage extends TcpDiscoveryAbstractMessage implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryConnectionCheckMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNode Node created this message.
     */
    public TcpDiscoveryConnectionCheckMessage(TcpDiscoveryNode creatorNode) {
        super(creatorNode.id());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // This method has been left empty intentionally to keep message size at min.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // This method has been left empty intentionally to keep message size at min.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryConnectionCheckMessage.class, this, "super", super.toString());
    }
}