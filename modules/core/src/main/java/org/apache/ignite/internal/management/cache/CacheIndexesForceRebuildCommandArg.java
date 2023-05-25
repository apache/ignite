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
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"cacheNames", "groupNames"}, onlyOneOf = true, optional = false)
public class CacheIndexesForceRebuildCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(description = "Specify node for indexes rebuild", example = "nodeId")
    private UUID nodeId;

    /** */
    @Argument(description = "Comma-separated list of cache names for which indexes should be rebuilt",
        example = "cacheName1,...cacheNameN")
    private String[] cacheNames;

    /** */
    @Argument(description = "Comma-separated list of cache group names for which indexes should be rebuilt",
        example = "groupName1,...groupNameN")
    private String[] groupNames;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        U.writeArray(out, cacheNames);
        U.writeArray(out, groupNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        cacheNames = U.readArray(in, String.class);
        groupNames = U.readArray(in, String.class);
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public String[] cacheNames() {
        return cacheNames;
    }

    /** */
    public void cacheNames(String[] cacheNames) {
        this.cacheNames = cacheNames;
    }

    /** */
    public String[] groupNames() {
        return groupNames;
    }

    /** */
    public void groupNames(String[] groupNames) {
        this.groupNames = groupNames;
    }
}
