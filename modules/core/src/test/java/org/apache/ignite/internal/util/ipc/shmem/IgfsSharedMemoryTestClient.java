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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.OutputStream;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;

/**
 * Test-purposed app launching {@link IpcSharedMemoryClientEndpoint} and designed
 * to be used with conjunction to {@link GridJavaProcess}.
 */
public class IgfsSharedMemoryTestClient {
    /**
     * Internal protocol message prefix saying that the next text in the outputted line
     * are comma-separated shared memory ids.
     */
    static final String SHMEM_IDS_MSG_PREFIX = "SHMEM_IDS_MSG_PREFIX";

    /**
     * @param args Args.
     */
    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    public static void main(String[] args) {
        X.println("Starting client ...");

        // Tell our process PID to the wrapper.
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        OutputStream os = null;

        try {
            IpcSharedMemoryClientEndpoint client = (IpcSharedMemoryClientEndpoint) IpcEndpointFactory.connectEndpoint(
                    "shmem:" + IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, new JavaLogger());

            os = client.outputStream();

            // Tell our shmem ids.
            X.println(SHMEM_IDS_MSG_PREFIX + client.inSpace().sharedMemoryId() + "," +
                client.outSpace().sharedMemoryId());

            for (;;) {
                X.println("Write: 123");

                os.write(123);

                Thread.sleep(IpcSharedMemoryCrashDetectionSelfTest.RW_SLEEP_TIMEOUT);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            U.closeQuiet(os);
        }
    }
}