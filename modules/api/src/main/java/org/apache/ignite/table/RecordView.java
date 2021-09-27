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

package org.apache.ignite.table;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.mapper.Mappers;
import org.apache.ignite.tx.Transaction;

/**
 * Record view of table provides methods to access table records.
 * <p>
 *
 * @param <R> Record type.
 * @apiNote 'Record class field' &gt;-&lt; 'table column' mapping laid down in implementation.
 * @apiNote Some methods require a record with the only key fields set. This is not mandatory requirement
 * and value fields will be just ignored.
 * @see Mappers
 */
public interface RecordView<R> extends TableView<R> {
    /**
     * Fills given record with the values from the table.
     * Similar to {@link #get(Object)}, but return original object with filled value fields.
     * <p>
     * All value fields of given object will be rewritten.
     *
     * @param recObjToFill Record object with key fields to be filled.
     * @return Record with all fields filled from the table.
     */
    R fill(R recObjToFill);

    /**
     * Asynchronously fills given record with the values from the table.
     * Similar to {@link #get(Object)}, but return original object with filled value fields.
     * <p>
     * All value fields of given object will be rewritten.
     *
     * @param recObjToFill Record object with key fields to be filled.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<R> fillAsync(R recObjToFill);

    /** {@inheritDoc} */
    @Override RecordView<R> withTransaction(Transaction tx);
}
