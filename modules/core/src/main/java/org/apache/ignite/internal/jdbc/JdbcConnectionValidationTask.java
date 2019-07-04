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
package org.apache.ignite.internal.jdbc;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * This task is used for {@link org.apache.ignite.internal.jdbc.JdbcConnection} validation.
 *
 * @deprecated Using Ignite client node based JDBC driver is preferable.
 * See documentation of {@link org.apache.ignite.IgniteJdbcDriver} for details.
 */
@Deprecated
public class JdbcConnectionValidationTask extends ComputeTaskSplitAdapter<Object, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteException {
        return Collections.singletonList(new ComputeJobAdapter() {
            @Override public Object execute() throws IgniteException {
                return true;
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        return F.first(results).getData();
    }
}
