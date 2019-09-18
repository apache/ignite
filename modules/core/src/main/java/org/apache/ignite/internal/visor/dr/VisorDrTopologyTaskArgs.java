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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public class VisorDrTopologyTaskArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int SENDER_HUBS_FLAG = 0x01;
    /** */
    public static final int RECEIVER_HUBS_FLAG = 0x02;
    /** */
    public static final int DATA_NODES_FLAG = 0x04;
    /** */
    public static final int OTHER_NODES_FLAG = 0x08;

    /** */
    private int flags;

    /** Default constructor. */
    public VisorDrTopologyTaskArgs() {
    }

    /** */
    public boolean senderHubs() {
        return (flags & SENDER_HUBS_FLAG) != 0;
    }

    /** */
    public boolean receiverHubs() {
        return (flags & RECEIVER_HUBS_FLAG) != 0;
    }

    /** */
    public boolean dataNodes() {
        return (flags & DATA_NODES_FLAG) != 0;
    }

    /** */
    public boolean otherNodes() {
        return (flags & OTHER_NODES_FLAG) != 0;
    }

    /** */
    public VisorDrTopologyTaskArgs(int flags) {
        assert flags != 0;

        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(flags);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        flags = in.readInt();
    }
}
