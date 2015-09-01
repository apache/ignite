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

package org.apache.ignite.loadtests.direct.newnodes;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeTaskSession;

/**
 * Single split on new nodes test job target.
 */
public class GridSingleSplitNewNodesTestJobTarget {
    /**
     * @param level Level.
     * @param jobSes Job session.
     * @return Always returns {@code 1}.
     */
    @SuppressWarnings("unused")
    public int executeLoadTestJob(int level, ComputeTaskSession jobSes) {
        assert level > 0;
        assert jobSes != null;

        try {
            assert "1".equals(jobSes.waitForAttribute("1st", 10000));

            assert "2".equals(jobSes.waitForAttribute("2nd", 10000));
        }
        catch (InterruptedException e) {
            // Fail.
            throw new IgniteException("Failed to wait for attribute.", e);
        }

        return 1;
    }
}