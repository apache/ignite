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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

/**
 *
 */
public class TxRow extends TxKey {
    /** */
    private byte state;

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @param state Transaction state.
     */
    TxRow(long major, long minor, byte state) {
        super(major, minor);

        this.state = state;
    }

    /**
     * @return Transaction state.
     */
    public byte state() {
        return state;
    }
}
