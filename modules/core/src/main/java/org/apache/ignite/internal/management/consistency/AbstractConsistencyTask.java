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

package org.apache.ignite.internal.management.consistency;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/**
 * Base class for Consistency tasks.
 *
 * @param <A> Task argument type.
 * @param <J> Job result type
 */
public abstract class AbstractConsistencyTask<A, J> extends VisorMultiNodeTask<A, ConsistencyTaskResult, J> {
    /** {@inheritDoc} */
    @Override protected ConsistencyTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        ConsistencyTaskResult taskRes = new ConsistencyTaskResult();
        StringBuilder sb = new StringBuilder();

        for (ComputeJobResult res : results) {
            if (res.isCancelled())
                taskRes.cancelled(true);

            Exception e = res.getException();

            if (e != null) {
                taskRes.failed(true);

                sb.append("Node: ").append(res.getNode()).append("\n")
                    .append("  Exception: ").append(e).append("\n")
                    .append(X.getFullStackTrace(e)).append("\n");
            }

            String data = res.getData();

            if (data != null)
                sb.append("Node: ").append(res.getNode()).append("\n")
                    .append("  Result: ").append(data).append("\n");
        }

        if (sb.length() != 0)
            taskRes.message(sb.toString());

        return taskRes;
    }
}
