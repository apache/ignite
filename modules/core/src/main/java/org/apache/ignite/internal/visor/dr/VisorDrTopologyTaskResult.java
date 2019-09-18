/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class VisorDrTopologyTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Data center id. */
    private byte dataCenterId;
    /** Server nodes count. */
    private int serverNodesCnt;
    /** Client nodes count. */
    private int clientNodesCnt;
    /** Sender hubs. */
    private List<T3<UUID, String, String>> senderHubs;
    /** Receiver hubs. */
    private List<T3<UUID, String, String>> receiverHubs;
    /** Data nodes. */
    private List<T2<UUID, String>> dataNodes;
    /** Other nodes. */
    private List<T3<UUID, String, String>> otherNodes;

    /**
     * Default constructor.
     */
    public VisorDrTopologyTaskResult() {
        // No-op.
    }

    /** */
    public VisorDrTopologyTaskResult(
        byte dataCenterId,
        int serverNodesCnt,
        int clientNodesCnt,
        List<T3<UUID, String, String>> senderHubs,
        List<T3<UUID, String, String>> receiverHubs,
        List<T2<UUID, String>> dataNodes,
        List<T3<UUID, String, String>> otherNodes
    ) {
        this.dataCenterId = dataCenterId;
        this.serverNodesCnt = serverNodesCnt;
        this.clientNodesCnt = clientNodesCnt;
        this.senderHubs = senderHubs;
        this.receiverHubs = receiverHubs;
        this.dataNodes = dataNodes;
        this.otherNodes = otherNodes;
    }

    /** */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /** */
    public int getServerNodesCount() {
        return serverNodesCnt;
    }

    /** */
    public int getClientNodesCount() {
        return clientNodesCnt;
    }

    /** */
    public List<T3<UUID, String, String>> getSenderHubs() {
        return senderHubs;
    }

    /** */
    public List<T3<UUID, String, String>> getReceiverHubs() {
        return receiverHubs;
    }

    /** */
    public List<T2<UUID, String>> getDataNodes() {
        return dataNodes;
    }

    /** */
    public List<T3<UUID, String, String>> getOtherNodes() {
        return otherNodes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);

        out.writeInt(serverNodesCnt);
        out.writeInt(clientNodesCnt);

        U.writeCollection(out, senderHubs);
        U.writeCollection(out, receiverHubs);
        U.writeCollection(out, dataNodes);
        U.writeCollection(out, otherNodes);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();

        serverNodesCnt = in.readInt();
        clientNodesCnt = in.readInt();

        senderHubs = U.readList(in);
        receiverHubs = U.readList(in);
        dataNodes = U.readList(in);
        otherNodes = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrTopologyTaskResult.class, this);
    }
}
