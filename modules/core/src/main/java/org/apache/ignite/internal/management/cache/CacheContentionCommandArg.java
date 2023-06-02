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
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CacheContentionCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** Min queue size. */
    @Positional
    @Argument(example = "minQueueSize")
    private int minQueueSize;

    /** Node id. */
    @Positional
    @Argument(optional = true, example = "nodeId")
    private UUID nodeId;

    /** Max print. */
    @Positional
    @Argument(optional = true, example = "maxPrint")
    private int maxPrint = 10;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        out.writeInt(minQueueSize);
        out.writeInt(maxPrint);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        minQueueSize = in.readInt();
        maxPrint = in.readInt();
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
    public int minQueueSize() {
        return minQueueSize;
    }

    /** */
    public void minQueueSize(int minQueueSize) {
        this.minQueueSize = minQueueSize;
    }

    /** */
    public int maxPrint() {
        return maxPrint;
    }

    /** */
    public void maxPrint(int maxPrint) {
        this.maxPrint = maxPrint;
    }
}
