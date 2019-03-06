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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Helper class for {@link IpcSharedMemoryNativeLoaderSelfTest#testLoadWithCorruptedLibFile()} test, which contains test logic.
 */
public class LoadWithCorruptedLibFileTestRunner {
    public static final String TMP_DIR_FOR_TEST = System.getProperty("user.home");
    public static final String LOADED_LIB_FILE_NAME = System.mapLibraryName(IpcSharedMemoryNativeLoader.LIB_NAME);

    /**
     * Do logic of test.
     *
     * @param args Args.
     * @throws Exception If test failed.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("java.io.tmpdir", TMP_DIR_FOR_TEST);

        createCorruptedLibFile();

        IpcSharedMemoryNativeLoader.load(null);
    }

    /**
     * Create corrupted library file.
     *
     * @throws IOException If an I/O error occurs.
     */
    private static void createCorruptedLibFile() throws IOException {
        File libFile = new File(System.getProperty("java.io.tmpdir"), LOADED_LIB_FILE_NAME);

        if (libFile.exists() && !libFile.delete())
            throw new IllegalStateException("Could not delete loaded lib file.");

        libFile.deleteOnExit();

        if (!libFile.createNewFile())
            throw new IllegalStateException("Could not create new file.");

        try (FileOutputStream out = new FileOutputStream(libFile)) {
            out.write("Corrupted file information instead of good info.\n".getBytes());
        }
    }
}