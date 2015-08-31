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

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.List;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;

import static org.apache.ignite.compute.ComputeJobResultPolicy.FAILOVER;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Test task summarizes length of all strings in the arguments list.
 * <p>
 * The argument of the task is JSON-serialized array of objects to calculate string length sum of.
 */
public class ClientHttpTask extends ComputeTaskSplitAdapter<String, Integer> {
    /** Task delegate. */
    private final ClientTcpTask delegate = new ClientTcpTask();

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
        JSON json = JSONSerializer.toJSON(arg);

        List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

        //noinspection unchecked
        return delegate.split(gridSize, list);
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        return delegate.reduce(results);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }
}