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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Rotated (when page has been recycled) id part delta record.
 */
public class RotatedIdPartRecord extends PageDeltaRecord {
    /** Rotated id part. */
    private byte rotatedIdPart;

    /**
     * @param grpId Group id.
     * @param pageId Page id.
     * @param rotatedIdPart Rotated id part.
     */
    public RotatedIdPartRecord(int grpId, long pageId, int rotatedIdPart) {
        super(grpId, pageId);

        assert rotatedIdPart >= 0 && rotatedIdPart <= 0xFF;

        this.rotatedIdPart = (byte) rotatedIdPart;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageIO.setRotatedIdPart(pageAddr, rotatedIdPart);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.ROTATED_ID_PART_RECORD;
    }

    /**
     * @return Rotated id part.
     */
    public byte rotatedIdPart() {
        return rotatedIdPart;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RotatedIdPartRecord.class, this, "super", super.toString());
    }
}
