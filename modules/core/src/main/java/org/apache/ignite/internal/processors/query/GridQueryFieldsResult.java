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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;

/**
 * Field query result. It is composed of
 * fields metadata and iterator over queried fields.
 */
public interface GridQueryFieldsResult {
    /**
     * Gets metadata for queried fields.
     *
     * @return Meta data for queried fields.
     */
    List<GridQueryFieldMetadata> metaData();

    /**
     * Gets iterator over queried fields.
     *
     * @return Iterator over queried fields.
     */
    IgniteSpiCloseableIterator<List<?>> iterator() throws IgniteCheckedException;
}