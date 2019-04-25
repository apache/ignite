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

package org.apache.ignite.internal.binary.mutabletest;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class GridBinaryMarshalerAwareTestClass implements Binarylizable {
    /** */
    public String s;

    /** */
    public String sRaw;

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("s", s);

        BinaryRawWriter raw = writer.rawWriter();

        raw.writeString(sRaw);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        s = reader.readString("s");

        BinaryRawReader raw = reader.rawReader();

        sRaw = raw.readString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        return this == other || GridTestUtils.deepEquals(this, other);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridBinaryMarshalerAwareTestClass.class, this);
    }
}
