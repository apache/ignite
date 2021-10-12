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

package org.apache.ignite.internal.table;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Internal tables facade provides low-level methods for table operations.
 */
public interface IgniteTablesInternal {
    /**
     * Gets a table by id.
     *
     * @param id Table ID.
     * @return Table or {@code null} when not exists.
     */
    TableImpl table(IgniteUuid id);

    /**
     * Gets a table future by id. If the table exists, the future will point to it, otherwise to {@code null}.
     *
     * @param id Table id.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<TableImpl> tableAsync(IgniteUuid id);
}
