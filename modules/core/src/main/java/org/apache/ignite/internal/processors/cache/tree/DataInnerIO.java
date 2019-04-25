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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 *
 */
public final class DataInnerIO extends AbstractDataInnerIO {
    /** */
    public static final IOVersions<DataInnerIO> VERSIONS = new IOVersions<>(
        new DataInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private DataInnerIO(int ver) {
        super(T_DATA_REF_INNER, ver, true, 12);
    }
}
