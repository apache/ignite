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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;

/**
 * Kill job task.
 */
public class HadoopProtocolKillJobTask extends HadoopProtocolTaskAdapter<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Boolean run(ComputeJobContext jobCtx, Hadoop hadoop,
        HadoopProtocolTaskArguments args) throws IgniteCheckedException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);

        assert nodeId != null;
        assert id != null;

        HadoopJobId jobId = new HadoopJobId(nodeId, id);

        return hadoop.kill(jobId);
    }
}