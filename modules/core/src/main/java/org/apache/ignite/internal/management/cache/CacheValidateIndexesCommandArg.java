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
public class CacheValidateIndexesCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(example = "cacheName1,...,cacheNameN", optional = true)
    private String value;

    /** */
    @Positional
    @Argument(example = "nodeId", optional = true)
    private String value2;

    /** */
    private String[] caches;

    /** */
    private UUID[] nodeIds;

    /** */
    @Argument(example = "N", description = "validate only the first N keys", optional = true)
    private int checkFirst = -1;

    /** */
    @Argument(example = "K", description = "validate every Kth key", optional = true)
    private int checkThrough = -1;

    /** */
    @Argument(description = "check the CRC-sum of pages stored on disk", optional = true)
    private boolean checkCrc;

    /** */
    @Argument(description = "check that index size and cache size are the same", optional = true)
    private boolean checkSizes;

    /** */
    private static void ensurePositive(int numVal, String arg) {
        if (numVal <= 0)
            throw new IllegalArgumentException("Value for '" + arg + "' property should be positive.");
    }

    /** */
    private void parse(String value) {
        try {
            nodeIds = CommandUtils.parseVal(value, UUID[].class);

            return;
        }
        catch (IllegalArgumentException ignored) {
            //No-op.
        }

        caches = CommandUtils.parseVal(value, String[].class);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, value);
        U.writeString(out, value2);
        U.writeArray(out, caches);
        U.writeArray(out, nodeIds);
        out.writeInt(checkFirst);
        out.writeInt(checkThrough);
        out.writeBoolean(checkCrc);
        out.writeBoolean(checkSizes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        value = U.readString(in);
        value2 = U.readString(in);
        caches = U.readArray(in, String.class);
        nodeIds = U.readArray(in, UUID.class);
        checkFirst = in.readInt();
        checkThrough = in.readInt();
        checkCrc = in.readBoolean();
        checkSizes = in.readBoolean();
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
    public String value() {
        return value;
    }

    /** */
    public void value(String value) {
        this.value = value;

        parse(value);
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
    public String[] caches() {
        return caches;
    }

    /** */
    public void caches(String[] caches) {
        this.caches = caches;
    }

    /** */
    public int checkFirst() {
        return checkFirst;
    }

    /** */
    public void checkFirst(int checkFirst) {
        if (this.checkFirst == checkFirst)
            return;

        ensurePositive(checkFirst, "--check-first");

        this.checkFirst = checkFirst;
    }

    /** */
    public int checkThrough() {
        return checkThrough;
    }

    /** */
    public void checkThrough(int checkThrough) {
        if (this.checkThrough == checkThrough)
            return;

        ensurePositive(checkThrough, "--check-through");

        this.checkThrough = checkThrough;
    }

    /** */
    public boolean checkCrc() {
        return checkCrc;
    }

    /** */
    public void checkCrc(boolean checkCrc) {
        this.checkCrc = checkCrc;
    }

    /** */
    public boolean checkSizes() {
        return checkSizes;
    }

    /** */
    public void checkSizes(boolean checkSizes) {
        this.checkSizes = checkSizes;
    }
}
