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
import org.apache.ignite.internal.processors.hadoop.*;

import java.util.*;

import static org.apache.ignite.internal.processors.hadoop.GridHadoopJobPhase.*;

/**
 * Submit job task.
 */
public class GridHadoopProtocolSubmitJobTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(ComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws IgniteCheckedException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);
        GridHadoopDefaultJobInfo info = args.get(2);

        assert nodeId != null;
        assert id != null;
        assert info != null;

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        hadoop.submit(jobId, info);

        GridHadoopJobStatus res = hadoop.status(jobId);

        if (res == null) // Submission failed.
            res = new GridHadoopJobStatus(jobId, info.jobName(), info.user(), 0, 0, 0, 0, PHASE_CANCELLING, true, 1);

        return res;
    }
}
