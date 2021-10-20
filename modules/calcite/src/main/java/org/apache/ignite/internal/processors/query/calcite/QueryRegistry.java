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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

/**
 *
 */
public interface QueryRegistry<Row> extends Service {
    /**
     * Register the query or return the exists query with the same identifier.
     *
     * @param qry Query to register.
     * @return registered query.
     */
    Query<Row> register(Query<Row> qry);

    /**
     * Lookup query by identifier.
     *
     * @param id Query identified.
     * @return registered query or {@code null} if the query with specified identifier isn't found.
     */
    Query<Row> query(UUID id);

    /**
     * Unregister query by identifier.
     *
     * @param id Query identifier.
     */
    void unregister(UUID id);

    /** */
    Collection<Query<Row>> runningQueries();
}
