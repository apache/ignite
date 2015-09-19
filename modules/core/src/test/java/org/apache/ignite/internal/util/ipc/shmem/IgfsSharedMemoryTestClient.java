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