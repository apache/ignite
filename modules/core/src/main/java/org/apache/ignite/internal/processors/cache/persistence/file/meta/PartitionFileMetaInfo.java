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

package org.apache.ignite.internal.processors.cache.persistence.file.meta;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class PartitionFileMetaInfo implements FileMetaInfo {
    /** */
    private Integer grpId;

    /** */
    private Integer partId;

    /** */
    private String name;

    /** */
    private Long size;

    /** */
    private Integer type;

    /** */
    public PartitionFileMetaInfo() {
        this(null, null, null, null, null);
    }

    /**
     * @param grpId Cache group identifier.
     * @param name Cache partition file name.
     * @param size Cache partition file size.
     * @param type {@code 0} partition file, {@code 1} delta file.
     */
    public PartitionFileMetaInfo(Integer grpId, Integer partId, String name, Long size, Integer type) {
        this.grpId = grpId;
        this.partId = partId;
        this.name = name;
        this.size = size;
        this.type = type;
    }

    /**
     * @return Related cache group id.
     */
    public Integer getGrpId() {
        return grpId;
    }

    /**
     * @return Cache partition id.
     */
    public Integer getPartId() {
        return partId;
    }

    /**
     * @return Partition file name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Partition file size.
     */
    public Long getSize() {
        return size;
    }

    /**
     * @return {@code 0} partition file, {@code 1} delta file.
     */
    public Integer getType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public void readMetaInfo(DataInputStream is) throws IOException {
        grpId = is.readInt();
        partId = is.readInt();
        name = is.readUTF();
        size = is.readLong();
        type = is.readInt();

        if (grpId == null || partId == null || name == null || size == null || type == null)
            throw new IOException("File meta information incorrect: " + this);
    }

    /** {@inheritDoc} */
    @Override public void writeMetaInfo(DataOutputStream os) throws IOException {
        if (grpId == null || partId == null || name == null || size == null || type == null)
            throw new IOException("File meta information incorrect: " + this);

        os.writeInt(grpId);
        os.writeInt(partId);
        os.writeUTF(name);
        os.writeLong(size);
        os.writeInt(type);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionFileMetaInfo.class, this);
    }
}
