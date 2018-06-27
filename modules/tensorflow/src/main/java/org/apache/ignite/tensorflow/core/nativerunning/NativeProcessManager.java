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

package org.apache.ignite.tensorflow.core.nativerunning;

import java.io.Serializable;
import java.util.function.Supplier;
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
    /** */
    private static final long serialVersionUID = 718119807915504045L;

    /**
     * Constructs a new native process manager.
     *
     * @param igniteSupplier Ignite instance supplier.
     * @param <T> Type of serializable supplier.
     */
    public <T extends Supplier<Ignite> & Serializable> NativeProcessManager(T igniteSupplier) {
        super(new LongRunningProcessManager(igniteSupplier));
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
