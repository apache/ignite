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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test shared memory native loader.
 */
public class IpcSharedMemoryNativeLoaderSelfTest {
    /**
     * Test {@link IpcSharedMemoryNativeLoader#load()} in case, when native library path was
     * already loaded, but corrupted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLoadWithCorruptedLibFile() throws Exception {
        if (U.isWindows())
            return;

        Process ps = GridJavaProcess.exec(
            LoadWithCorruptedLibFileTestRunner.class,
            null,
            null,
            null,
            null,
            Collections.<String>emptyList(),
            System.getProperty("surefire.test.class.path")
        ).getProcess();

        readStreams(ps);

        int code = ps.waitFor();

        assertEquals("Returned code have to be 0.", 0, code);
    }

    /**
     * Read information from process streams.
     *
     * @param proc Process.
     * @throws IOException If an I/O error occurs.
     */
    private void readStreams(Process proc) throws IOException {
        BufferedReader stdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        String s;

        while ((s = stdOut.readLine()) != null)
            System.out.println("OUT>>>>>> " + s);

        BufferedReader errOut = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        while ((s = errOut.readLine()) != null)
            System.out.println("ERR>>>>>> " + s);
    }
}
