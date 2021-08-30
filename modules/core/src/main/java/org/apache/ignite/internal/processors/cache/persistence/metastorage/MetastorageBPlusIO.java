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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/** */
public interface MetastorageBPlusIO {
    /** IO versions for metastorage inner nodes. */
    public static final IOVersions<MetastorageInnerIO> INNER_IO_VERSIONS = new IOVersions<>(
        new MetastorageInnerIO(1),
        new MetastorageInnerIO(2)
    );

    /** IO versions for metastorage leaf nodes. */
    public static final IOVersions<MetastorageLeafIO> LEAF_IO_VERSIONS = new IOVersions<>(
        new MetastorageLeafIO(1),
        new MetastorageLeafIO(2)
    );

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Row link.
     */
    public long getLink(long pageAddr, int idx);

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Key size in bytes.
     */
    public short getKeySize(long pageAddr, int idx);

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Key.
     */
    public String getKey(long pageAddr, int idx, MetastorageRowStore rowStore) throws IgniteCheckedException;

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param rowStore Row store.
     * @return Data row.
     */
    MetastorageDataRow getDataRow(long pageAddr, int idx, MetastorageRowStore rowStore) throws IgniteCheckedException;
}
