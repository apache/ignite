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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.ipc.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.*;

import java.io.*;

/**
 * Test-purposed app launching {@link IpcSharedMemoryServerEndpoint} and designed
 * to be used with conjunction to {@link GridJavaProcess}.
 */
public class IgfsSharedMemoryTestServer {
    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    public static void main(String[] args) throws IgniteCheckedException {
        System.out.println("Starting server ...");

        U.setWorkDirectory(null, U.getIgniteHome());

        // Tell our process PID to the wrapper.
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        InputStream is = null;

        try {
            IpcServerEndpoint srv = new IpcSharedMemoryServerEndpoint();

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
