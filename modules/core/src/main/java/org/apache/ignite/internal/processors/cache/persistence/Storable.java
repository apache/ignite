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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 * Simple interface for data, store in some RowStore.
 */
public interface Storable {
    /**
     * @param link Link for this row.
     */
    public void link(long link);

    /**
     * @return Link for this row.
     */
    public long link();

    /**
     * @return Partition.
     */
    public int partition();

    /**
     * @return Row size in page.
     * @throws IgniteCheckedException If failed.
     */
    public int size() throws IgniteCheckedException;

    /**
     * @return Row header size in page. Header is indivisible part of row
     * which is entirely available on the very first page followed by the row link.
     */
    public int headerSize();

    /**
     * @return I/O for handling this storable.
     */
    public IOVersions<? extends AbstractDataPageIO> ioVersions();
}
