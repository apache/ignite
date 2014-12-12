/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Example that shows usage of {@link org.apache.ignite.IgniteFs} API. It starts a GridGain node with {@code GGFS}
 * configured and performs several file system operations (create, write, append, read and delete
 * files, create, list and delete directories).
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * GGFS: {@code 'ggstart.sh examples/config/filesystem/example-ggfs.xml'}.
 * <p>
 * Alternatively you can run {@link GgfsNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/filesystem/example-ggfs.xml} configuration.
 */
public final class GgfsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Ignite g = Ignition.start("examples/config/filesystem/example-ggfs.xml");

        System.out.println();
        System.out.println(">>> GGFS example started.");

        try {
            // Get an instance of GridGain File System.
            IgniteFs fs = g.fileSystem("ggfs");

            // Working directory path.
            IgniteFsPath workDir = new IgniteFsPath("/examples/ggfs");

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
     * @param fs GGFS.
     * @param path File or directory path.
     * @throws IgniteCheckedException In case of error.
     */
    private static void delete(IgniteFs fs, IgniteFsPath path) throws IgniteCheckedException {
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
     * @param fs GGFS.
     * @param path Directory path.
     * @throws IgniteCheckedException In case of error.
     */
    private static void mkdirs(IgniteFs fs, IgniteFsPath path) throws IgniteCheckedException {
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
     * @param fs GGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteCheckedException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void create(IgniteFs fs, IgniteFsPath path, @Nullable byte[] data)
        throws IgniteCheckedException, IOException {
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
     * @param fs GGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteCheckedException If file can't be created.
     * @throws IOException If data can't be written.
     */
    private static void append(IgniteFs fs, IgniteFsPath path, byte[] data) throws IgniteCheckedException, IOException {
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
     * @param fs GGFS.
     * @param path File path.
     * @throws IgniteCheckedException If file can't be opened.
     * @throws IOException If data can't be read.
     */
    private static void read(IgniteFs fs, IgniteFsPath path) throws IgniteCheckedException, IOException {
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
     * @param fs GGFS.
     * @param path Directory path.
     * @throws IgniteCheckedException In case of error.
     */
    private static void list(IgniteFs fs, IgniteFsPath path) throws IgniteCheckedException {
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
     * @param fs GGFS.
     * @param path File or directory path.
     * @throws IgniteCheckedException In case of error.
     */
    private static void printInfo(IgniteFs fs, IgniteFsPath path) throws IgniteCheckedException {
        System.out.println();
        System.out.println("Information for " + path + ": " + fs.info(path));
    }
}
