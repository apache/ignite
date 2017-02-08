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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX12_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX12_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX16_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX16_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX20_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX20_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX24_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX24_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX28_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX28_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX32_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX32_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX4_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX4_REF_LEAF;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX8_REF_INNER;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.T_H2_EX8_REF_LEAF;

/**
 *
 */
public abstract class H2ExtrasIOHelper {

    /**
     * Maximum payload size.
     */
    public static final int MAX_SIZE = 32;

    /** */
    public static IOVersions<? extends H2ExtrasInnerIO> getInnerIOForSize(int size) {
        if (size <= 4)
            return H2ExtrasInnerIO.VERSIONS_4;
        else if (size <= 8)
            return H2ExtrasInnerIO.VERSIONS_8;
        else if (size <= 12)
            return H2ExtrasInnerIO.VERSIONS_12;
        else if (size <= 16)
            return H2ExtrasInnerIO.VERSIONS_16;
        else if (size <= 20)
            return H2ExtrasInnerIO.VERSIONS_20;
        else if (size <= 24)
            return H2ExtrasInnerIO.VERSIONS_24;
        else if (size <= 28)
            return H2ExtrasInnerIO.VERSIONS_28;
        else if (size <= 32)
            return H2ExtrasInnerIO.VERSIONS_32;

        return null;
    }

    /** */
    public static IOVersions<? extends H2ExtrasLeafIO> getLeafIOForSize(int size) {
        if (size <= 4)
            return H2ExtrasLeafIO.VERSIONS_4;
        else if (size <= 8)
            return H2ExtrasLeafIO.VERSIONS_8;
        else if (size <= 12)
            return H2ExtrasLeafIO.VERSIONS_12;
        else if (size <= 16)
            return H2ExtrasLeafIO.VERSIONS_16;
        else if (size <= 20)
            return H2ExtrasLeafIO.VERSIONS_20;
        else if (size <= 24)
            return H2ExtrasLeafIO.VERSIONS_24;
        else if (size <= 28)
            return H2ExtrasLeafIO.VERSIONS_28;
        else if (size <= 32)
            return H2ExtrasLeafIO.VERSIONS_32;

        return null;
    }

    /** */
    public static void registerInnerIO() {
        PageIO.registerH2ExtraInner(T_H2_EX4_REF_INNER, H2ExtrasInnerIO.VERSIONS_4);
        PageIO.registerH2ExtraInner(T_H2_EX8_REF_INNER, H2ExtrasInnerIO.VERSIONS_8);
        PageIO.registerH2ExtraInner(T_H2_EX12_REF_INNER, H2ExtrasInnerIO.VERSIONS_12);
        PageIO.registerH2ExtraInner(T_H2_EX16_REF_INNER, H2ExtrasInnerIO.VERSIONS_16);
        PageIO.registerH2ExtraInner(T_H2_EX20_REF_INNER, H2ExtrasInnerIO.VERSIONS_20);
        PageIO.registerH2ExtraInner(T_H2_EX24_REF_INNER, H2ExtrasInnerIO.VERSIONS_24);
        PageIO.registerH2ExtraInner(T_H2_EX28_REF_INNER, H2ExtrasInnerIO.VERSIONS_28);
        PageIO.registerH2ExtraInner(T_H2_EX32_REF_INNER, H2ExtrasInnerIO.VERSIONS_32);
    }

    /** */
    public static void registerLeafIO() {
        PageIO.registerH2ExtraLeaf(T_H2_EX4_REF_LEAF, H2ExtrasLeafIO.VERSIONS_4);
        PageIO.registerH2ExtraLeaf(T_H2_EX8_REF_LEAF, H2ExtrasLeafIO.VERSIONS_8);
        PageIO.registerH2ExtraLeaf(T_H2_EX12_REF_LEAF, H2ExtrasLeafIO.VERSIONS_12);
        PageIO.registerH2ExtraLeaf(T_H2_EX16_REF_LEAF, H2ExtrasLeafIO.VERSIONS_16);
        PageIO.registerH2ExtraLeaf(T_H2_EX20_REF_LEAF, H2ExtrasLeafIO.VERSIONS_20);
        PageIO.registerH2ExtraLeaf(T_H2_EX24_REF_LEAF, H2ExtrasLeafIO.VERSIONS_24);
        PageIO.registerH2ExtraLeaf(T_H2_EX28_REF_LEAF, H2ExtrasLeafIO.VERSIONS_28);
        PageIO.registerH2ExtraLeaf(T_H2_EX32_REF_LEAF, H2ExtrasLeafIO.VERSIONS_32);
    }
}
