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

package org.apache.ignite.internal.visor.snapshot;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for single node visor snapshot task.
 */
public abstract class VisorSnapshotOneNodeTask<A, R> extends VisorMultiNodeTask<A, VisorSnapshotTaskResult, R> {
    /** {@inheritDoc} */
    @Nullable @Override protected VisorSnapshotTaskResult reduce0(List<ComputeJobResult> results) {
        assert results.size() == 1 : results.size();

        ComputeJobResult res = F.first(results);

        return new VisorSnapshotTaskResult(res.getData(), res.getException());
    }
}
