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

package org.apache.ignite.examples.igfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.jetbrains.annotations.Nullable;

/**
 * Example that shows usage of {@link org.apache.ignite.IgniteFileSystem} API. It starts a node with {@code IgniteFs}
 * configured and performs several file system operations (create, write, append, read and delete
 * files, create, list and delete directories).
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * IGFS: {@code 'ignite.sh examples/config/filesystem/example-igfs.xml'}.
 * <p>
 * Alternatively you can run {@link IgfsNodeStartup} in another JVM which will start
 * node with {@code examples/config/filesystem/example-igfs.xml} configuration.
 */
public final class IgfsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Ignite ignite = Ignition.start("examples/config/filesystem/example-igfs.xml");

        System.out.println();
        System.out.println(">>> IGFS example started.");

        try {
            // Get an instance of Ignite File System.
            IgniteFileSystem fs = ignite.fileSystem("igfs");

            // Working directory path.
            IgfsPath workDir = new IgfsPath("/examples/fs");

            // Cleanup working directory.
            delete(fs, workDir);

            // Create empty working directory.
            mkdirs(fs, workDir);

            // Print information for working directory.
            printInfo(fs, workDir);

            // File path.
            IgfsPath filePath = new IgfsPath(workDir, "file.txt");

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
                create(fs, new IgfsPath(workDir, "file-" + i + ".txt"), null);

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
     * @param fs IGFS.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    private static void delete(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        if (fs.exists(path)) {
            boolean isFile = fs.info(path).isFile();

            try {
                fs.delete(path, true);

                System.out.println();
                System.out.println(">>> Deleted " + (isFile ? "file" : "directory") + ": " + path);
            }
            catch (IgfsException e) {
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
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    private static void mkdirs(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        try {
            fs.mkdirs(path);

            System.out.println();
            System.out.println(">>> Created directory: " + path);
        }
        catch (IgfsException e) {
            System.out.println();
            System.out.println(">>> Failed to create a directory [path=" + path + ", msg=" + e.getMessage() + ']');
        }

        System.out.println();
    }

    /**
     * Creates file and writes provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void create(IgniteFileSystem fs, IgfsPath path, @Nullable byte[] data)
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
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void append(IgniteFileSystem fs, IgfsPath path, byte[] data) throws IgniteException, IOException {
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
    private static void read(IgniteFileSystem fs, IgfsPath path) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();

        byte[] data = new byte[(int)fs.info(path).length()];

        try (IgfsInputStream in = fs.open(path)) {
            in.read(data);
        }

        System.out.println();
        System.out.println(">>> Read data from " + path + ": " + Arrays.toString(data));
    }

    /**
     * Lists files in directory.
     *
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
    private static void list(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isDirectory();

        Collection<IgfsPath> files = fs.listPaths(path);

        if (files.isEmpty()) {
            System.out.println();
            System.out.println(">>> No files in directory: " + path);
        }
        else {
            System.out.println();
            System.out.println(">>> List of files in directory: " + path);

            for (IgfsPath f : files)
                System.out.println(">>>     " + f.name());
        }

        System.out.println();
    }

    /**
     * Prints information for file or directory.
     *
     * @param fs IGFS.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
    private static void printInfo(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        System.out.println();
        System.out.println("Information for " + path + ": " + fs.info(path));
    }
}