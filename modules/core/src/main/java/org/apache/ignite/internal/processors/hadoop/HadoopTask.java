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

package org.apache.ignite.internal.processors.hadoop;

import java.io.Externalizable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Hadoop task.
 */
public abstract class HadoopTask {
    /** */
    private HadoopTaskInfo taskInfo;

    /**
     * Creates task.
     *
     * @param taskInfo Task info.
     */
    protected HadoopTask(HadoopTaskInfo taskInfo) {
        assert taskInfo != null;

        this.taskInfo = taskInfo;
    }

    /**
     * For {@link Externalizable}.
     */
    @SuppressWarnings("ConstructorNotProtectedInAbstractClass")
    public HadoopTask() {
        // No-op.
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public HadoopTaskInfo info() {
        return taskInfo;
    }

    /**
     * Runs task.
     *
     * @param taskCtx Context.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void run(HadoopTaskContext taskCtx) throws IgniteCheckedException;

    /**
     * Interrupts task execution.
     */
    public abstract void cancel();
}