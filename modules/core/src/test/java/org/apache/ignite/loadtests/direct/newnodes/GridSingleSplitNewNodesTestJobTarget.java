/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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