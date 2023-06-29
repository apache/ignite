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

package org.apache.ignite.internal.management.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.CliConfirmArgument;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;

/** */
@CliConfirmArgument
@ArgumentGroup(value = {"servers", "clients", "nodes"}, onlyOneOf = true, optional = true)
public class TxCommandArg extends TxCommand.AbstractTxCommandArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(example = "XID", optional = true)
    private String xid;

    /** */
    @Argument(example = "SECONDS", optional = true)
    private Long minDuration;

    /** */
    @Argument(example = "SIZE", optional = true)
    private Integer minSize;

    /** */
    @Argument(example = "PATTERN_REGEX", optional = true)
    private String label;

    /** */
    @Argument
    private boolean servers;

    /** */
    @Argument
    private boolean clients;

    /** */
    @Argument(example = "consistentId1[,consistentId2,....,consistentIdN]")
    private String[] nodes;

    /** */
    @Argument(optional = true, example = "NUMBER")
    private Integer limit;

    /** */
    @Argument(optional = true, description = "Output order")
    @EnumDescription(
        names = {
            "DURATION",
            "SIZE",
            "START_TIME"
        },
        descriptions = {
            "Sort by duration",
            "Sort by size",
            "Sort by start time"
        }
    )
    private VisorTxSortOrder order;

    /** */
    @Argument(optional = true)
    private boolean kill;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, xid);
        out.writeObject(minDuration);
        out.writeObject(minSize);
        U.writeString(out, label);
        out.writeBoolean(servers);
        out.writeBoolean(clients);
        U.writeArray(out, nodes);
        out.writeObject(limit);
        U.writeEnum(out, order);
        out.writeBoolean(kill);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        xid = U.readString(in);
        minDuration = (Long)in.readObject();
        minSize = (Integer)in.readObject();
        label = U.readString(in);
        servers = in.readBoolean();
        clients = in.readBoolean();
        nodes = U.readArray(in, String.class);
        limit = (Integer)in.readObject();
        order = U.readEnum(in, VisorTxSortOrder.class);
        kill = in.readBoolean();
    }

    /** */
    public String xid() {
        return xid;
    }

    /** */
    public void xid(String xid) {
        this.xid = xid;
    }

    /** */
    public Long minDuration() {
        return minDuration;
    }

    /** */
    public void minDuration(Long minDuration) {
        A.ensure(minDuration == null || minDuration > 0, "--min-duration");

        if (minDuration != null)
            this.minDuration = minDuration * 1000;
    }

    /** */
    public Integer minSize() {
        return minSize;
    }

    /** */
    public void minSize(Integer minSize) {
        A.ensure(minSize == null || minSize > 0, "--min-size");

        this.minSize = minSize;
    }

    /** */
    public String label() {
        return label;
    }

    /** */
    public void label(String label) {
        if (label != null) {
            try {
                Pattern.compile(label);
            }
            catch (PatternSyntaxException ignored) {
                throw new IllegalArgumentException("Illegal regex syntax");
            }
        }

        this.label = label;
    }

    /** */
    public boolean servers() {
        return servers;
    }

    /** */
    public void servers(boolean servers) {
        this.servers = servers;
    }

    /** */
    public boolean clients() {
        return clients;
    }

    /** */
    public void clients(boolean clients) {
        this.clients = clients;
    }

    /** */
    public String[] nodes() {
        return nodes;
    }

    /** */
    public void nodes(String[] nodes) {
        this.nodes = nodes;
    }

    /** */
    public Integer limit() {
        return limit;
    }

    /** */
    public void limit(Integer limit) {
        A.ensure(limit == null || limit > 0, "--limit");

        this.limit = limit;
    }

    /** */
    public VisorTxSortOrder order() {
        return order;
    }

    /** */
    public void order(VisorTxSortOrder order) {
        this.order = order;
    }

    /** */
    public boolean kill() {
        return kill;
    }

    /** */
    public void kill(boolean kill) {
        this.kill = kill;
    }
}
