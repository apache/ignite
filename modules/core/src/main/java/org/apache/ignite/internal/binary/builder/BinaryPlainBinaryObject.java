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

package org.apache.ignite.internal.binary.builder;

import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.binary.BinaryObject;

/**
 *
 */
public class BinaryPlainBinaryObject implements BinaryLazyValue {
    /** */
    private final BinaryObject binaryObj;

    /**
     * @param binaryObj Binary object.
     */
    public BinaryPlainBinaryObject(BinaryObject binaryObj) {
        this.binaryObj = binaryObj;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return binaryObj;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        BinaryObject val = binaryObj;

        if (val instanceof BinaryObjectOffheapImpl)
            val = ((BinaryObjectOffheapImpl)val).heapCopy();

        writer.doWriteBinaryObject((BinaryObjectImpl)val);
    }
}
