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

package org.apache.ignite.internal.processors.hadoop.proto;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.hadoop.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;

/**
 * Job status task.
 */
public class GridHadoopProtocolJobStatusTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default poll delay */
    private static final long DFLT_POLL_DELAY = 100L;

    /** Attribute for held status. */
    private static final String ATTR_HELD = "held";

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(final ComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws IgniteCheckedException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);
        Long pollDelay = args.get(2);

        assert nodeId != null;
        assert id != null;

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        if (pollDelay == null)
            pollDelay = DFLT_POLL_DELAY;

        if (pollDelay > 0) {
            IgniteInternalFuture<?> fut = hadoop.finishFuture(jobId);

            if (fut != null) {
                if (fut.isDone() || F.eq(jobCtx.getAttribute(ATTR_HELD), true))
                    return hadoop.status(jobId);
                else {
                    fut.listenAsync(new IgniteInClosure<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> fut0) {
                            jobCtx.callcc();
                        }
                    });

                    jobCtx.setAttribute(ATTR_HELD, true);

                    return jobCtx.holdcc(pollDelay);
                }
            }
            else
                return null;
        }
        else
            return hadoop.status(jobId);
    }
}
