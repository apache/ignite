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

package org.apache.ignite.tensorflow.proc.pythonrunning;

import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.ignite.tensorflow.proc.ProcessManager;
import org.apache.ignite.tensorflow.proc.ProcessManagerWrapper;
import org.apache.ignite.tensorflow.proc.nativerunning.NativeProcess;
import org.apache.ignite.tensorflow.proc.nativerunning.NativeProcessManager;

/**
 * Python process manager that allows to  start, stop and make other actions with python processes.
 */
public class PythonProcessManager extends ProcessManagerWrapper<NativeProcess, PythonProcess> {
    /** */
    private static final long serialVersionUID = -7095409565854538504L;

    /**
     * Constructs a new instance of python process manager.
     */
    public PythonProcessManager() {
        this(new NativeProcessManager());
    }

    /**
     * Constructs a new instance of python process manager.
     *
     * @param delegate Delegate.
     */
    public PythonProcessManager(ProcessManager<NativeProcess> delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override protected NativeProcess transformSpecification(PythonProcess spec) {
        return new NativeProcess(
            (Supplier<ProcessBuilder> & Serializable) () -> new ProcessBuilder("python3", "-i"),
            spec.getStdin(),
            spec.getNodeId()
        );
    }
}
