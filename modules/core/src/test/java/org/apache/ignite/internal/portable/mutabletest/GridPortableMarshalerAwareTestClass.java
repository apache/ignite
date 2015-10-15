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

package org.apache.ignite.internal.portable.mutabletest;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableRawReader;
import org.apache.ignite.portable.PortableRawWriter;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class GridPortableMarshalerAwareTestClass implements PortableMarshalAware {
    /** */
    public String s;

    /** */
    public String sRaw;

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        writer.writeString("s", s);

        PortableRawWriter raw = writer.rawWriter();

        raw.writeString(sRaw);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        s = reader.readString("s");

        PortableRawReader raw = reader.rawReader();

        sRaw = raw.readString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("FloatingPointEquality")
    @Override public boolean equals(Object other) {
        return this == other || GridTestUtils.deepEquals(this, other);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPortableMarshalerAwareTestClass.class, this);
    }
}