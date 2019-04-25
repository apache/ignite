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

package org.apache.ignite.tensorflow.core.nativerunning;

import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.core.ProcessManager;
import org.apache.ignite.tensorflow.core.ProcessManagerWrapper;
import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcess;
import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcessManager;
import org.apache.ignite.tensorflow.core.nativerunning.task.NativeProcessStartTask;

/**
 * Native process manager that allows to start, stop and make other actions with native processes.
 */
public class NativeProcessManager extends ProcessManagerWrapper<LongRunningProcess, NativeProcess> {
    /**
     * Constructs a new native process manager.
     *
     * @param ignite Ignite instance.
     */
    public NativeProcessManager(Ignite ignite) {
        super(new LongRunningProcessManager(ignite));
    }

    /**
     * Constructs a new native process manager.
     *
     * @param delegate Delegate.
     */
    public NativeProcessManager(ProcessManager<LongRunningProcess> delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override protected LongRunningProcess transformSpecification(NativeProcess spec) {
        return new LongRunningProcess(spec.getNodeId(), new NativeProcessStartTask(spec));
    }
}
