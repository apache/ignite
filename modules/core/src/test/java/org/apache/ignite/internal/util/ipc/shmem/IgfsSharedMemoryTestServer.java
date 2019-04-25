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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.InputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.IgniteTestResources;

/**
 * Test-purposed app launching {@link IpcSharedMemoryServerEndpoint} and designed
 * to be used with conjunction to {@link GridJavaProcess}.
 */
public class IgfsSharedMemoryTestServer {
    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    public static void main(String[] args) throws IgniteCheckedException {
        System.out.println("Starting server ...");

        // Tell our process PID to the wrapper.
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        InputStream is = null;

        try {
            IpcServerEndpoint srv = new IpcSharedMemoryServerEndpoint(U.defaultWorkDirectory());

            new IgniteTestResources().inject(srv);

            srv.start();

            System.out.println("IPC shared memory server endpoint started");

            IpcEndpoint clientEndpoint = srv.accept();

            is = clientEndpoint.inputStream();

            for (;;) {
                X.println("Before read.");

                is.read();

                Thread.sleep(IpcSharedMemoryCrashDetectionSelfTest.RW_SLEEP_TIMEOUT);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            U.closeQuiet(is);
        }
    }
}