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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite distinguishes between array of objects and array of enums.
 * This extension of {@link BinaryArray} intended to keep correct typeId for binary enum arrays.
 */
class BinaryEnumArray extends BinaryArray {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * {@link Externalizable} support.
     */
    public BinaryEnumArray() {
    }

    /**
     * @param ctx Context.
     * @param compTypeId Component type id.
     * @param compClsName Component class name.
     * @param arr Array.
     */
    public BinaryEnumArray(BinaryContext ctx, int compTypeId,
        @Nullable String compClsName, Object[] arr) {
        super(ctx, compTypeId, compClsName, arr);
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return GridBinaryMarshaller.ENUM_ARR;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return new BinaryEnumArray(ctx, compTypeId, compClsName, arr.clone());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryEnumArray.class, this, super.toString());
    }
}
