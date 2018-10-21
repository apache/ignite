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
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for {@link DataRegionMetrics}
 */
public class VisorMemoryMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String name;

    /** */
    private long totalAllocatedPages;

    /** */
    private float allocationRate;

    /** */
    private float evictionRate;

    /** */
    private float largeEntriesPagesPercentage;

    /** */
    private float pagesFillFactor;

    /** */
    private long dirtyPages;

    /** */
    private float pagesReplaceRate;

    /** */
    private long physicalMemoryPages;

    /** */
    private long totalAllocatedSz;

    /** */
    private long physicalMemSz;

    /** */
    private int pageSize;

    /** */
    private long cpBufSz;

    /** */
    private long cpUsedBufPages;

    /** */
    private long cpUsedBufSz;

    /** */
    private long pagesRead;

    /** */
    private long pagesWritten;

    /** */
    private long pagesReplaced;

    /** */
    private long offHeapSz;

    /** */
    private long offHeapUsedSz;

    /**
     * Default constructor.
     */
    public VisorMemoryMetrics() {
        // No-op.
    }

    /**
     * @param m Metrics instance to create DTO.
     */
    public VisorMemoryMetrics(DataRegionMetrics m) {
        name = m.getName();
        totalAllocatedPages = m.getTotalAllocatedPages();
        allocationRate = m.getAllocationRate();
        evictionRate = m.getEvictionRate();
        largeEntriesPagesPercentage = m.getLargeEntriesPagesPercentage();
        pagesFillFactor = m.getPagesFillFactor();
        dirtyPages = m.getDirtyPages();
        pagesReplaceRate = m.getPagesReplaceRate();
        physicalMemoryPages = m.getPhysicalMemoryPages();
        totalAllocatedSz = m.getTotalAllocatedSize();
        physicalMemSz = m.getPhysicalMemorySize();

        pageSize = m.getPageSize();

        cpBufSz = m.getCheckpointBufferSize();
        cpUsedBufPages = m.getUsedCheckpointBufferPages();
        cpUsedBufSz = m.getUsedCheckpointBufferSize();

        pagesRead = m.getPagesRead();
        pagesWritten = m.getPagesWritten();
        pagesReplaced = m.getPagesReplaced();

        offHeapSz = m.getOffHeapSize();
        offHeapUsedSz = m.getOffheapUsedSize();
    }

    /**
     * @return Name of the memory region.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Total number of allocated pages.
     */
    public long getTotalAllocatedPages() {
        return totalAllocatedPages;
    }

    /**
     * @return Number of allocated pages per second.
     */
    public float getAllocationRate() {
        return allocationRate;
    }

    /**
     * @return Eviction rate.
     */
    public float getEvictionRate() {
        return evictionRate;
    }

    /**
     * @return Number of evicted pages per second.
     */
    public float getLargeEntriesPagesPercentage() {
        return largeEntriesPagesPercentage;
    }

    /**
     * @return Percentage of pages fully occupied by large entities.
     */
    public float getPagesFillFactor() {
        return pagesFillFactor;
    }

    /**
     * @return Current number of dirty pages.
     */
    public long getDirtyPages() {
        return dirtyPages;
    }

    /**
     * @return Pages per second replace rate.
     */
    public float getPagesReplaceRate() {
        return pagesReplaceRate;
    }

    /**
     * @return Total number of pages loaded to RAM.
     */
    public long getPhysicalMemoryPages() {
        return physicalMemoryPages;
    }


    /**
     * @return Total size of memory allocated, in bytes.
     */
    public long getTotalAllocatedSize() {
        return totalAllocatedSz;
    }

    /**
     * @return Total size of pages loaded to RAM in bytes.
     */
    public long getPhysicalMemorySize() {
        return physicalMemSz;
    }

    /**
     * This method needed for compatibility with V2.
     *
     * @return Used checkpoint buffer size in pages.
     */
    public long getCheckpointBufferPages() {
        return cpUsedBufPages;
    }

    /**
     * @return @return Checkpoint buffer size in bytes.
     */
    public long getCheckpointBufferSize() {
        return cpBufSz;
    }

    /**
     * @return Used checkpoint buffer size in pages.
     */
    public long getUsedCheckpointBufferPages() {
        return cpUsedBufPages;
    }

    /**
     * @return Used checkpoint buffer size in bytes.
     */
    public long getUsedCheckpointBufferSize() {
        return cpUsedBufSz;
    }

    /**
     * @return Page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return The number of read pages from last restart.
     */
    public long getPagesRead() {
        return pagesRead;
    }

    /**
     * @return The number of written pages from last restart.
     */
    public long getPagesWritten() {
        return pagesWritten;
    }

    /**
     * @return The number of replaced pages from last restart .
     */
    public long getPagesReplaced() {
        return pagesReplaced;
    }

    /**
     * @return Total offheap size in bytes.
     */
    public long getOffHeapSize() {
        return offHeapSz;
    }

    /**
     * @return Total used offheap size in bytes.
     */
    public long getOffheapUsedSize() {
        return offHeapUsedSz;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeLong(totalAllocatedPages);
        out.writeFloat(allocationRate);
        out.writeFloat(evictionRate);
        out.writeFloat(largeEntriesPagesPercentage);
        out.writeFloat(pagesFillFactor);
        out.writeLong(dirtyPages);
        out.writeFloat(pagesReplaceRate);
        out.writeLong(physicalMemoryPages);

        // V2
        out.writeLong(totalAllocatedSz);
        out.writeLong(physicalMemSz);
        out.writeLong(cpUsedBufPages);
        out.writeLong(cpBufSz);
        out.writeInt(pageSize);

        // V3
        out.writeLong(cpUsedBufSz);

        out.writeLong(pagesRead);
        out.writeLong(pagesWritten);
        out.writeLong(pagesReplaced);

        out.writeLong(offHeapSz);
        out.writeLong(offHeapUsedSz);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        totalAllocatedPages = in.readLong();
        allocationRate = in.readFloat();
        evictionRate = in.readFloat();
        largeEntriesPagesPercentage = in.readFloat();
        pagesFillFactor = in.readFloat();
        dirtyPages = in.readLong();
        pagesReplaceRate = in.readFloat();
        physicalMemoryPages = in.readLong();

        if (protoVer > V1) {
            totalAllocatedSz = in.readLong();
            physicalMemSz = in.readLong();
            cpUsedBufPages = in.readLong();
            cpBufSz = in.readLong();
            pageSize = in.readInt();
        }

        if (protoVer > V2) {
            cpUsedBufSz = in.readLong();

            pagesRead = in.readLong();
            pagesWritten = in.readLong();
            pagesReplaced = in.readLong();

            offHeapSz = in.readLong();
            offHeapUsedSz = in.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorMemoryMetrics.class, this);
    }
}
