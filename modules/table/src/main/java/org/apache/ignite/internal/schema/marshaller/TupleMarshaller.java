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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Tuple marshaller interface.
 */
public interface TupleMarshaller {
    /**
     * Marshals tuple.
     *
     * @param tuple Record tuple.
     * @return Table row with columns set from given tuples.
     */
    Row marshal(@NotNull Tuple tuple);

    /**
     * Marshal tuple key part only.
     *
     * @param tuple Record tuple with key columns only.
     * @return Table row with columns set from given tuples.
     */
    Row marshalKey(@NotNull Tuple tuple);

    /**
     * Marshals KV pair.
     *
     * @param keyTuple Key tuple.
     * @param valTuple Value tuple.
     * @return Table row with columns set from given tuples.
     */
    Row marshal(@NotNull Tuple keyTuple, Tuple valTuple);
}
