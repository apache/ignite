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
 * Result for {@link VisorCacheClearTask}.
 */
public class VisorCacheClearTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache size before clearing. */
    private long sizeBefore;

    /** Cache size after clearing. */
    private long sizeAfter;

    /**
     * Default constructor.
     */
    public VisorCacheClearTaskResult() {
        // No-op.
    }

    /**
     * @param sizeBefore Cache size before clearing.
     * @param sizeAfter Cache size after clearing.
     */
    public VisorCacheClearTaskResult(long sizeBefore, long sizeAfter) {
        this.sizeBefore = sizeBefore;
        this.sizeAfter = sizeAfter;
    }

    /**
     * @return Cache size before clearing.
     */
    public long getSizeBefore() {
        return sizeBefore;
    }

    /**
     * @return Cache size after clearing.
     */
    public long getSizeAfter() {
        return sizeAfter;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(sizeBefore);
        out.writeLong(sizeAfter);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sizeBefore = in.readLong();
        sizeAfter = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheClearTaskResult.class, this);
    }
}
