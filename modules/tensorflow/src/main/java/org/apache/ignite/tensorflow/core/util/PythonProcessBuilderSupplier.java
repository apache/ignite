/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tensorflow.core.util;

import java.lang.management.ManagementFactory;
import java.util.Map;
import org.apache.ignite.tensorflow.util.SerializableSupplier;

/**
 * Python process builder supplier that is used to create Python process builder.
 */
public class PythonProcessBuilderSupplier implements SerializableSupplier<ProcessBuilder> {
    /** */
    private static final long serialVersionUID = 7181937306294456125L;

    /** Python environment variable name. */
    private static final String PYTHON_ENV_NAME = "PYTHON";

    /** Interactive flag (allows to used standard input to pass Python script). */
    private final boolean interactive;

    /** Meta information that adds to script as arguments. */
    private final String[] meta;

    /**
     * Constructs a new instance of Python process builder supplier.
     *
     * @param interactive Interactive flag (allows to used standard input to pass Python script).
     * @param meta Meta information that adds to script as arguments.
     */
    public PythonProcessBuilderSupplier(boolean interactive, String... meta) {
        this.interactive = interactive;
        this.meta = meta;
    }

    /**
     * Returns process builder to be used to start Python process.
     *
     * @return Process builder to be used to start Python process.
     */
    public ProcessBuilder get() {
        String python = System.getenv(PYTHON_ENV_NAME);

        if (python == null)
            python = "python3";

        ProcessBuilder procBldr;
        if (interactive) {
            String[] cmd = new String[meta.length + 3];

            cmd[0] = python;
            cmd[1] = "-i";
            cmd[2] = "-";

            System.arraycopy(meta, 0, cmd, 3, meta.length);

            procBldr = new ProcessBuilder(cmd);
        }
        else
            procBldr = new ProcessBuilder(python);

        Map<String, String> env = procBldr.environment();
        env.put("PPID", String.valueOf(getProcessId()));

        return procBldr;
    }

    /**
     * Returns current process identifier.
     *
     * @return Process identifier.
     */
    private long getProcessId() {
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

        return Long.parseLong(pid);
    }
}
