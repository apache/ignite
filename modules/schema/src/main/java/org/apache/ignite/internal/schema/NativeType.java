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

package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.tostring.S;

/**
 * A thin wrapper over {@link NativeTypeSpec} to instantiate parameterized constrained types.
 */
public class NativeType implements Comparable<NativeType> {
    /** */
    public static final NativeType BYTE = new NativeType(NativeTypeSpec.BYTE, 1);

    /** */
    public static final NativeType SHORT = new NativeType(NativeTypeSpec.SHORT, 2);

    /** */
    public static final NativeType INTEGER = new NativeType(NativeTypeSpec.INTEGER, 4);

    /** */
    public static final NativeType LONG = new NativeType(NativeTypeSpec.LONG, 8);

    /** */
    public static final NativeType FLOAT = new NativeType(NativeTypeSpec.FLOAT, 4);

    /** */
    public static final NativeType DOUBLE = new NativeType(NativeTypeSpec.DOUBLE, 8);

    /** */
    public static final NativeType UUID = new NativeType(NativeTypeSpec.UUID, 16);

    /** */
    public static final NativeType STRING = new NativeType(NativeTypeSpec.STRING);

    /** */
    public static final NativeType BYTES = new NativeType(NativeTypeSpec.BYTES);

    /** */
    private final NativeTypeSpec typeSpec;

    /** Type length. */
    private final int len;

    /**
     *
     */
    protected NativeType(NativeTypeSpec typeSpec, int len) {
        if (!typeSpec.fixedLength())
            throw new IllegalArgumentException("Size must be provided only for fixed-length types: " + typeSpec);

        if (len <= 0)
            throw new IllegalArgumentException("Size must be positive [typeSpec=" + typeSpec + ", size=" + len + ']');

        this.typeSpec = typeSpec;
        this.len = len;
    }

    /**
     *
     */
    protected NativeType(NativeTypeSpec typeSpec) {
        if (typeSpec.fixedLength())
            throw new IllegalArgumentException("Fixed-length types must be created by the " +
                "length-aware constructor: " + typeSpec);

        this.typeSpec = typeSpec;
        this.len = 0;
    }

    /**
     * @return Length of the type if it is a fixlen type. For varlen types the return value is undefined, so the user
     * should explicitly check {@code spec().fixedLength()} before using this method.
     *
     * @see NativeTypeSpec#fixedLength()
     */
    public int length() {
        return len;
    }

    /**
     * @return Type specification enum.
     */
    public NativeTypeSpec spec() {
        return typeSpec;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NativeType that = (NativeType)o;

        return len == that.len && typeSpec == that.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = typeSpec.hashCode();

        res = 31 * res + len;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        // Fixed-sized types go first.
        if (len <= 0 && o.len > 0)
            return 1;

        if (len > 0 && o.len <= 0)
            return -1;

        // Either size is -1 for both, or positive for both. Compare sizes, then description.
        int cmp = Integer.compare(len, o.len);

        if (cmp != 0)
            return cmp;

        return typeSpec.name().compareTo(o.typeSpec.name());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NativeType.class.getSimpleName(),
            "name", typeSpec.name(),
            "len", len,
            "fixed", typeSpec.fixedLength());
    }
}
