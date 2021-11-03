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
 * Variable-length native type.
 */
public class VarlenNativeType extends NativeType {
    /** Length of the type. */
    private final int len;

    /**
     * @param typeSpec Type spec.
     * @param len      Type length.
     */
    protected VarlenNativeType(NativeTypeSpec typeSpec, int len) {
        super(typeSpec);

        this.len = len;
    }

    /** {@inheritDoc} */
    @Override
    public boolean mismatch(NativeType type) {
        return super.mismatch(type) || len < ((VarlenNativeType) type).len;
    }

    /**
     * @return Length of the type.
     */
    public int length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(VarlenNativeType.class.getSimpleName(), "name", spec(), "len", len);
    }
}
