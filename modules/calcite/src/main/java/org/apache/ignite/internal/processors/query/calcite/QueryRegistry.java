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
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

/**
 * Registry of the running queries.
 */
public interface QueryRegistry extends Service {
    /**
     * Register the query or return the existing one with the same identifier.
     *
     * @param qry Query to register.
     * @return Registered query.
     */
    RunningQuery register(RunningQuery qry);

    /**
     * Lookup query by identifier.
     *
     * @param id Query identified.
     * @return Registered query or {@code null} if the query with specified identifier isn't found.
     */
    RunningQuery query(UUID id);

    /**
     * Unregister query by identifier.
     *
     * @param id Query identifier.
     */
    void unregister(UUID id);

    /** */
    Collection<? extends RunningQuery> runningQueries();
}
