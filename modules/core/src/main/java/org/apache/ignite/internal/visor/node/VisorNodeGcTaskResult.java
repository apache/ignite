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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for task returns GC execution results.
 */
public class VisorNodeGcTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Size before GC execution. */
    private long sizeBefore;

    /** Size after GC execution. */
    private long sizeAfter;

    /**
     * Default constructor.
     */
    public VisorNodeGcTaskResult() {
        // No-op.
    }

    /**
     * @param sizeBefore Size before GC execution.
     * @param sizeAfter Size after GC execution.
     */
    public VisorNodeGcTaskResult(long sizeBefore, long sizeAfter) {
        this.sizeBefore = sizeBefore;
        this.sizeAfter = sizeAfter;
    }

    /**
     * @return Size before GC execution.
     */
    public Long getSizeBefore() {
        return sizeBefore;
    }

    /**
     * @return Size after GC execution.
     */
    public Long getSizeAfter() {
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
        return S.toString(VisorNodeGcTaskResult.class, this);
    }
}
