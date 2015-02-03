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

package org.apache.ignite.examples.fs;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Example that shows usage of {@link IgniteFs} API. It starts a node with {@code IgniteFs}
 * configured and performs several file system operations (create, write, append, read and delete
 * files, create, list and delete directories).
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * IgniteFs: {@code 'ignite.sh examples/config/filesystem/example-ignitefs.xml'}.
 * <p>
 * Alternatively you can run {@link IgniteFsNodeStartup} in another JVM which will start
 * node with {@code examples/config/filesystem/example-ignitefs.xml} configuration.
 */
public final class IgniteFsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Ignite ignite = Ignition.start("examples/config/filesystem/example-ignitefs.xml");

        System.out.println();
        System.out.println(">>> Ignitefs example started.");

        try {
            // Get an instance of Ignite File System.
            org.apache.ignite.IgniteFs fs = ignite.fileSystem("ignitefs");

            // Working directory path.
            IgniteFsPath workDir = new IgniteFsPath("/examples/fs");

            // Cleanup working directory.
            delete(fs, workDir);

            // Create empty working directory.
            mkdirs(fs, workDir);

            // Print information for working directory.
            printInfo(fs, workDir);

            // File path.
            IgniteFsPath filePath = new IgniteFsPath(workDir, "file.txt");

            // Create file.
            create(fs, filePath, new byte[] {1, 2, 3});

            // Print information for file.
            printInfo(fs, filePath);

            // Append more data to previously created file.
            append(fs, filePath, new byte[] {4, 5});

            // Print information for file.
            printInfo(fs, filePath);

            // Read data from file.
            read(fs, filePath);

            // Delete file.
            delete(fs, filePath);

            // Print information for file.
            printInfo(fs, filePath);

            // Create several files.
            for (int i = 0; i < 5; i++)
                create(fs, new IgniteFsPath(workDir, "file-" + i + ".txt"), null);

            list(fs, workDir);
        }
        finally {
            Ignition.stop(false);
        }
    }

    /**
     * Deletes file or directory. If directory
     * is not empty, it's deleted recursively.
     *
     * @param fs IgniteFs.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    private static void delete(org.apache.ignite.IgniteFs fs, IgniteFsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        if (fs.exists(path)) {
            boolean isFile = fs.info(path).isFile();

            try {
                fs.delete(path, true);

                System.out.println();
                System.out.println(">>> Deleted " + (isFile ? "file" : "directory") + ": " + path);
            }
            catch (IgniteFsException e) {
                System.out.println();
                System.out.println(">>> Failed to delete " + (isFile ? "file" : "directory") + " [path=" + path +
                    ", msg=" + e.getMessage() + ']');
            }
        }
        else {
            System.out.println();
            System.out.println(">>> Won't delete file or directory (doesn't exist): " + path);
        }
    }

    /**
     * Creates directories.
     *
     * @param fs IgniteFs.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    private static void mkdirs(org.apache.ignite.IgniteFs fs, IgniteFsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        try {
            fs.mkdirs(path);

            System.out.println();
            System.out.println(">>> Created directory: " + path);
        }
        catch (IgniteFsException e) {
            System.out.println();
            System.out.println(">>> Failed to create a directory [path=" + path + ", msg=" + e.getMessage() + ']');
        }

        System.out.println();
    }

    /**
     * Creates file and writes provided data to it.
     *
     * @param fs IgniteFs.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void create(org.apache.ignite.IgniteFs fs, IgniteFsPath path, @Nullable byte[] data)
        throws IgniteException, IOException {
        assert fs != null;
        assert path != null;

        try (OutputStream out = fs.create(path, true)) {
            System.out.println();
            System.out.println(">>> Created file: " + path);

            if (data != null) {
                out.write(data);

                System.out.println();
                System.out.println(">>> Wrote data to file: " + path);
            }
        }

        System.out.println();
    }

    /**
     * Opens file and appends provided data to it.
     *
     * @param fs IgniteFs.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void append(org.apache.ignite.IgniteFs fs, IgniteFsPath path, byte[] data) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert data != null;
        assert fs.info(path).isFile();

        try (OutputStream out = fs.append(path, true)) {
            System.out.println();
            System.out.println(">>> Opened file: " + path);

            out.write(data);
        }

        System.out.println();
        System.out.println(">>> Appended data to file: " + path);
    }

    /**
     * Opens file and reads it to byte array.
     *
     * @param fs IgniteFs.
     * @param path File path.
     * @throws IgniteException If file can't be opened.
     * @throws IOException If data can't be read.
     */
    private static void read(org.apache.ignite.IgniteFs fs, IgniteFsPath path) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();

        byte[] data = new byte[(int)fs.info(path).length()];

        try (IgniteFsInputStream in = fs.open(path)) {
            in.read(data);
        }

        System.out.println();
        System.out.println(">>> Read data from " + path + ": " + Arrays.toString(data));
    }

    /**
     * Lists files in directory.
     *
     * @param fs IgniteFs.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    private static void list(org.apache.ignite.IgniteFs fs, IgniteFsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isDirectory();

        Collection<IgniteFsPath> files = fs.listPaths(path);

        if (files.isEmpty()) {
            System.out.println();
            System.out.println(">>> No files in directory: " + path);
        }
        else {
            System.out.println();
            System.out.println(">>> List of files in directory: " + path);

            for (IgniteFsPath f : files)
                System.out.println(">>>     " + f.name());
        }

        System.out.println();
    }

    /**
     * Prints information for file or directory.
     *
     * @param fs IgniteFs.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    private static void printInfo(org.apache.ignite.IgniteFs fs, IgniteFsPath path) throws IgniteException {
        System.out.println();
        System.out.println("Information for " + path + ": " + fs.info(path));
    }
}
