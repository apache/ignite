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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CacheFindGarbageCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(optional = true, example = "groupName1,...,groupNameN")
    private String value;

    /** */
    @Positional
    @Argument(optional = true, example = "nodeId")
    private String value2;

    /** */
    private String[] groups;

    /** */
    private UUID[] nodeIds;

    /** */
    @Argument(optional = true)
    private boolean delete;

    /** */
    private void parse(String value) {
        try {
            nodeIds = CommandUtils.parseVal(value, UUID[].class);

            return;
        }
        catch (IllegalArgumentException ignored) {
            //No-op.
        }

        groups = CommandUtils.parseVal(value, String[].class);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, value);
        U.writeString(out, value2);
        U.writeArray(out, groups);
        U.writeArray(out, nodeIds);
        out.writeBoolean(delete);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        value = U.readString(in);
        value2 = U.readString(in);
        groups = U.readArray(in, String.class);
        nodeIds = U.readArray(in, UUID.class);
        delete = in.readBoolean();
    }

    /** */
    public UUID[] nodeIds() {
        return nodeIds;
    }

    /** */
    public void nodeIds(UUID[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public String value() {
        return value;
    }

    /** */
    public void value(String value) {
        this.value = value;

        parse(value);
    }

    /** */
    public String value2() {
        return value2;
    }

    /** */
    public void value2(String value2) {
        this.value2 = value2;

        parse(value2);
    }

    /** */
    public boolean delete() {
        return delete;
    }

    /** */
    public void delete(boolean delete) {
        this.delete = delete;
    }

    /** */
    public String[] groups() {
        return groups;
    }

    /** */
    public void groups(String[] groups) {
        this.groups = groups;
    }
}
