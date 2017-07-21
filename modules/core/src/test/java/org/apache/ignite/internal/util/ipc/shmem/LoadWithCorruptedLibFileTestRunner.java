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