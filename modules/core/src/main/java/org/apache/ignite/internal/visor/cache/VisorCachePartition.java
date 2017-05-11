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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for information about keys in cache partition.
 */
public class VisorCachePartition extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int partId;

    /** */
    private long cnt;

    /**
     * Default constructor.
     */
    public VisorCachePartition() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param partId Partition id.
     * @param cnt Number of keys in partition.
     */
    public VisorCachePartition(int partId, long cnt) {
        this.partId = partId;
        this.cnt = cnt;
    }

    /**
     * @return Partition id.
     */
    public int getPartitionId() {
        return partId;
    }

    /**
     * @return Number of keys in partition.
     */
    public long getCount() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(partId);
        out.writeLong(cnt);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        partId = in.readInt();
        cnt = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCachePartition.class, this);
    }
}
