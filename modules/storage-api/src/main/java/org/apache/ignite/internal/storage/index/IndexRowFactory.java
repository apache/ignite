/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.storage.SearchRow;

/**
 * Temporary API for creating Index rows from a list of column values. All columns must be sorted according to the index columns order,
 * specified by the {@link SortedIndexDescriptor#indexRowColumns()}.
 */
public interface IndexRowFactory {
    /**
     * Creates an Index row from a list of column values.
     */
    IndexRow createIndexRow(Object[] columnValues, SearchRow primaryKey);

    /**
     * Creates an Prefix row from a list of column values.
     */
    IndexRowPrefix createIndexRowPrefix(Object[] prefixColumnValues);
}
