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

package org.apache.ignite.compute;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Job sibling interface defines a job from the same split. In other words a sibling is a job returned
 * from the same {@link ComputeTask#map(List, Object)} method invocation.
 */
public interface ComputeJobSibling {
    /**
     * Gets ID of this grid job sibling. Note that ID stays constant
     * throughout job life time, even if a job gets failed over to another
     * node.
     *
     * @return Job ID.
     */
    public IgniteUuid getJobId();

    /**
     * Sends a request to cancel this sibling.
     *
     * @throws IgniteException If cancellation message could not be sent.
     */
    public void cancel() throws IgniteException;
}