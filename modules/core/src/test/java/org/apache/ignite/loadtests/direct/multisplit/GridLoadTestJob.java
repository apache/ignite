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

package org.apache.ignite.loadtests.direct.multisplit;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Load test job.
 */
public class GridLoadTestJob extends ComputeJobAdapter {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /**
     * Constructor.
     * @param arg Argument.
     */
    public GridLoadTestJob(Integer arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Serializable execute() {
        Integer i = this.<Integer>argument(0);

        assert i != null && i > 0;

        if (i == 1)
            return new GridLoadTestJobTarget().executeLoadTestJob(1);

        assert ignite != null;

        ignite.compute().localDeployTask(GridLoadTestTask.class, GridLoadTestTask.class.getClassLoader());

        return (Integer) ignite.compute().execute(GridLoadTestTask.class.getName(), i);
    }
}