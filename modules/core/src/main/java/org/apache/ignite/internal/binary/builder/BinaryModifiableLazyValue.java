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

import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 *
 */
public class BinaryModifiableLazyValue extends BinaryAbstractLazyValue {
    /** */
    protected final int len;

    /**
     * @param reader
     * @param valOff
     * @param len
     */
    public BinaryModifiableLazyValue(BinaryBuilderReader reader, int valOff, int len) {
        super(reader, valOff);

        this.len = len;
    }

    /** {@inheritDoc} */
    @Override protected Object init() {
        return reader.reader().unmarshal(valOff);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        if (val == null)
            writer.write(reader.array(), valOff, len);
        else
            writer.writeObject(val);
    }
}
