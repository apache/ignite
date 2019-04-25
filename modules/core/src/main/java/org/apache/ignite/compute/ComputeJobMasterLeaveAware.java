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

import org.apache.ignite.IgniteException;

/**
 * Annotation for handling master node leave during job execution.
 * <p>
 * If {@link ComputeJob} concrete class implements this interface then in case when master node leaves topology
 * during job execution the callback method {@link #onMasterNodeLeft(ComputeTaskSession)} will be executed.
 * <p>
 * Implementing this interface gives you ability to preserve job execution result or its intermediate state
 * which could be reused later. E.g. you can save job execution result to the database or as a checkpoint
 * and reuse it when failed task is being executed again thus avoiding job execution from scratch.
 */
public interface ComputeJobMasterLeaveAware {
    /**
     * A method which is executed in case master node has left topology during job execution.
     *
     * @param ses Task session, can be used for checkpoint saving.
     * @throws IgniteException In case of any exception.
     */
    public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException;
}