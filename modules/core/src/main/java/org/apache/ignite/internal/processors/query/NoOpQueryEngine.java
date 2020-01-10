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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.jetbrains.annotations.Nullable;

/** No op implementation. */
public class NoOpQueryEngine extends GridProcessorAdapter implements QueryEngine {
    /**
     * @param ctx Kernal context.
     */
    public NoOpQueryEngine(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext ctx, String schemaName, String query, Object... params) throws IgniteSQLException {
        return Collections.emptyList();
    }
}
