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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;

/**
 * The exception indicates a duplicate type ID was encountered.
 */
public class DuplicateTypeIdException extends IgniteCheckedException {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Platform ID. */
    private final byte platformId;

    /** Type ID. */
    private final int typeId;

    /** Name of already registered class. */
    private final String oldClsName;

    /** Name of new class being registered. */
    private final String newClsName;

    /**
     * Initializes new instance of {@link DuplicateTypeIdException} class.
     *
     * @param platformId Platform ID.
     * @param typeId Type ID.
     * @param oldClsName Name of already registered class.
     * @param newClsName Name of new class being registered.
     */
    DuplicateTypeIdException(byte platformId, int typeId, String oldClsName, String newClsName) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.oldClsName = oldClsName;
        this.newClsName = newClsName;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return "Duplicate ID [platformId="
            + platformId
            + ", typeId="
            + typeId
            + ", oldCls="
            + oldClsName
            + ", newCls="
            + newClsName + ']';
    }

    /**
     * @return Name of already registered class.
     */
    public String getRegisteredClassName() {
        return oldClsName;
    }
}