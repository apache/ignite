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

import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.Mappers;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * Table view of table provides methods to access table records regarding binary object concept.
 * <p>
 * Provided different views (key-value vs record) and approaches (mapped-object vs binary) to reach the data.
 * <p>
 * Binary table views might be useful in cases (but not limited) if user key-value classes are not in classpath
 * or serialization/deserialization is unwanted due to performance reasons.
 *
 * @apiNote Some methods require a record with the only key columns set. This is not mandatory requirement
 * and value columns will be just ignored.
 * @see RecordView
 * @see Table
 * @see KeyValueView
 * @see KeyValueBinaryView
 */
public interface Table extends TableView<Tuple> {
    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    @NotNull String tableName();

    /**
     * Creates record view of table for record class mapper provided.
     *
     * @param recMapper Record class mapper.
     * @param <R> Record type.
     * @return Table record view.
     */
    <R> RecordView<R> recordView(RecordMapper<R> recMapper);

    /**
     * Creates key-value view of table for key-value class mappers provided.
     *
     * @param keyMapper Key class mapper.
     * @param valMapper Value class mapper.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Table key-value view.
     */
    <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper);

    /**
     * Creates key-value view of table regarding the binary object concept.
     *
     * @return Table key-value view.
     */
    KeyValueBinaryView kvView();

    /**
     * Creates a transactional view of the table.
     *
     * @param tx The transaction.
     * @return Transactional table view.
     * @see Transaction
     * @see IgniteTransactions
     */
    @Override Table withTransaction(Transaction tx);

    /**
     * Creates record view of table for record class provided.
     *
     * @param recCls Record class.
     * @param <R> Record type.
     * @return Table record view.
     */
    default <R> RecordView<R> recordView(Class<R> recCls) {
        return recordView(Mappers.ofRecordClass(recCls));
    }

    /**
     * Creates key-value view of table for key and value classes provided.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Table key-value view.
     */
    default <K, V> KeyValueView<K, V> kvView(Class<K> keyCls, Class<V> valCls) {
        return kvView(Mappers.ofKeyClass(keyCls), Mappers.ofValueClass(valCls));
    }
}
