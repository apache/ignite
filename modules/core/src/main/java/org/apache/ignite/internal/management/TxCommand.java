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

package org.apache.ignite.internal.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.Data;
import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;

/**
 * TODO: here I use primitives with default values. Original command uses wrapper type - Integer, etc.
 */
@Data
public class TxCommand extends CommandWithSubs {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Parameter(example = "XID", optional = true)
    private String xid;

    /** */
    @Parameter(example = "SECONDS", optional = true)
    private long minDuration = -1;

    /** */
    @Parameter(example = "SIZE", optional = true)
    private int minSize = -1;

    /** */
    @Parameter(example = "PATTERN_REGEX", optional = true)
    private String label;

    /** */
    @Parameter(optional = true)
    private boolean servers;

    /** */
    @Parameter(optional = true)
    private boolean clients;

    /** */
    @Parameter(optional = true, example = "consistentId1[,consistentId2,....,consistentIdN]")
    private List<String> nodes;

    /** */
    @Parameter(optional = true, example = "NUMBER")
    private int limit = -1;

    /** */
    @Parameter(optional = true)
    private VisorTxSortOrder order;

    /** */
    @Parameter(optional = true)
    private boolean kill;

    /** */
    @Parameter(optional = true)
    private boolean info;

    /** */
    @Parameter(optional = true, excludeFromDescription = true)
    private boolean yes;

    /** */
    public TxCommand() {
        register(TxInfoCommand::new);
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "List or kill transactions";
    }

    /** {@inheritDoc} */
    @Override public boolean canBeExecuted() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean positionalSubsName() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeString(out, xid);
        out.writeLong(minDuration);
        out.writeInt(minSize);
        U.writeString(out, label);
        out.writeBoolean(servers);
        out.writeBoolean(clients);
        U.writeCollection(out, nodes);
        out.writeInt(limit);
        U.writeEnum(out, order);
        out.writeBoolean(kill);
        out.writeBoolean(info);
        out.writeBoolean(yes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        xid = U.readString(in);
        minDuration = in.readLong();
        minSize = in.readInt();
        label = U.readString(in);
        servers = in.readBoolean();
        clients = in.readBoolean();
        nodes = U.readList(in);
        limit = in.readInt();
        order = U.readEnum(in, VisorTxSortOrder.class);
        kill = in.readBoolean();
        info = in.readBoolean();
        yes = in.readBoolean();
    }
}
