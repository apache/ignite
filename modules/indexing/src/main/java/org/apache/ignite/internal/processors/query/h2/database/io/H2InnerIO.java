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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 * Inner page for H2 row references.
 */
public class H2InnerIO extends AbstractH2InnerIO {
    /** */
    public static final IOVersions<H2InnerIO> VERSIONS = new IOVersions<>(
        new H2InnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private H2InnerIO(int ver) {
        super(T_H2_REF_INNER, ver, 8);
    }
}
