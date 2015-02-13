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

package org.apache.ignite.ignitefs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop file system load application.
 * <p>
 * Command line arguments:
 * <ul>
 * <li>-u {url} file system URL</li>
 * <li>-hadoopCfg {cfg} Hadoop configuration</li>
 * <li>-f {num} files number</li>
 * <li>-r {num} reads number</li>
 * <li>-w {num} writes number</li>
 * <li>-d {num} deletes number</li>
 * <li>-delay {delay} delay between operations in milliseconds</li>
 * <li>-t {num} threads number</li>
 * <li>-minSize {min size} min file size in bytes</li>
 * <li>-maxSize {max size} max file size in bytes</li>
 * <li>-startNode {true|false} if 'true' then starts node before execution</li>
 * <li>-nodeCfg {cfg} configuration for started node</li>
 * <li>-primaryOnly {true|false} if 'true' then creates files only in directory named 'primary' </li>
 * </ul>
 * Note: GGFS logging is disabled by default, to enable logging it is necessary to set flag
 * 'fs.ggfs.<name>.log.enabled' in Hadoop configuration file. By default log files are created in the
 * directory 'work/ggfs/log', this path can be changed in Hadoop configuration file using property
 * 'fs.ggfs.<name>.log.dir'.
 */
public class IgfsLoad {
    /** */
    private static final String DFLT_URL = "ggfs:///";

    /** */
    private static final int DFLT_MIN_FILE_SIZE = 100 * 1024;

    /** */
    private static final int DFLT_MAX_FILE_SIZE = 1024 * 1024;

    /** */
    private static final int DFLT_FILES_NUMBER = 1000;

    /** */
    private static final int DFLT_READS_NUMBER = 2000;

    /** */
    private static final int DFLT_WRITES_NUMBER = 2000;

    /** */
    private static final int DFLT_DELETES_NUMBER = 100;

    /** */
    private static final int DFLT_THREADS_NUMBER = 2;

    /** */
    private static final boolean DFLT_START_NODE = true;

    /** */
    private static final boolean DFLT_PRIMARY_ONLY = false;

    /** */
    private static final String DFLT_NODE_CFG = "config/hadoop/default-config.xml";

    /** */
    private static final long DFLT_DELAY = 5;

    /** */
    private static final String DFLT_HADOOP_CFG = "examples/config/filesystem/core-site.xml";

    /** */
    private static final int CREATE_BUF_SIZE = 100 * 1024;

    /** */
    private static final String DIR_PRIMARY_MODE = "primary";

    /** */
    private static final String DIR_PROXY_MODE = "proxy";

    /** */
    private static final String DIR_DUAL_SYNC_MODE = "dual_sync";

    /** */
    private static final String DIR_DUAL_ASYNC_MODE = "dual_async";

    /**
     * Main method.
     *
     * @param args Command line arguments.
     * @throws Exception If error occurs.
     */
    public static void main(String[] args) throws Exception {
        String url = DFLT_URL;

        int filesNum = DFLT_FILES_NUMBER;

        int minFileSize = DFLT_MIN_FILE_SIZE;

        int maxFileSize = DFLT_MAX_FILE_SIZE;

        int readsNum = DFLT_READS_NUMBER;

        int writesNum = DFLT_WRITES_NUMBER;

        int deletesNum = DFLT_DELETES_NUMBER;

        int threadsNum = DFLT_THREADS_NUMBER;

        long delay = DFLT_DELAY;

        String nodeCfg = DFLT_NODE_CFG;

        String hadoopCfg = DFLT_HADOOP_CFG;

        boolean startNode = DFLT_START_NODE;

        boolean primaryOnly = DFLT_PRIMARY_ONLY;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "-u":
                    url = args[++i];

                    break;

                case "-hadoopCfg":
                    hadoopCfg= args[++i];

                    break;

                case "-f":
                    filesNum = Integer.parseInt(args[++i]);

                    break;

                case "-r":
                    readsNum = Integer.parseInt(args[++i]);

                    break;

                case "-w":
                    writesNum = Integer.parseInt(args[++i]);

                    break;

                case "-minSize":
                    minFileSize = Integer.parseInt(args[++i]);

                    break;

                case "-maxSize":
                    maxFileSize = Integer.parseInt(args[++i]);

                    break;

                case "-d":
                    deletesNum = Integer.parseInt(args[++i]);

                    break;

                case "-t":
                    threadsNum = Integer.parseInt(args[++i]);

                    break;

                case "-delay":
                    delay = Long.parseLong(args[++i]);

                    break;

                case "-startNode":
                    startNode = Boolean.parseBoolean(args[++i]);

                    break;

                case "-nodeCfg":
                    nodeCfg= args[++i];

                    break;

                case "-primaryOnly":
                    primaryOnly = Boolean.parseBoolean(args[++i]);

                    break;
            }
        }

        X.println("File system URL: " + url);
        X.println("Hadoop configuration: " + hadoopCfg);
        X.println("Primary mode only: " + primaryOnly);
        X.println("Files number: " + filesNum);
        X.println("Reads number: " + readsNum);
        X.println("Writes number: " + writesNum);
        X.println("Deletes number: " + deletesNum);
        X.println("Min file size: " + minFileSize);
        X.println("Max file size: " + maxFileSize);
        X.println("Threads number: " + threadsNum);
        X.println("Delay: " + delay);

        if (minFileSize > maxFileSize)
            throw new IllegalArgumentException();

        Ignite ignite = null;

        if (startNode) {
            X.println("Starting node using configuration: " + nodeCfg);

            ignite = G.start(U.resolveIgniteUrl(nodeCfg));
        }

        try {
            new IgfsLoad().runLoad(url, hadoopCfg, primaryOnly, threadsNum, filesNum, readsNum, writesNum,
                deletesNum, minFileSize, maxFileSize, delay);
        }
        finally {
            if (ignite != null)
                G.stop(true);
        }
    }

    /**
     * Executes read/write/delete operations.
     *
     * @param url File system url.
     * @param hadoopCfg Hadoop configuration.
     * @param primaryOnly If {@code true} then creates files only on directory named 'primary'.
     * @param threads Threads number.
     * @param files Files number.
     * @param reads Reads number.
     * @param writes Writes number.
     * @param deletes Deletes number.
     * @param minSize Min file size.
     * @param maxSize Max file size.
     * @param delay Delay between operations.
     * @throws Exception If some file system operation failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    public void runLoad(String url, String hadoopCfg, final boolean primaryOnly, int threads, int files,
        final int reads, final int writes, final int deletes, final int minSize, final int maxSize, final long delay)
        throws Exception {
        Path fsPath = new Path(url);

        Configuration cfg = new Configuration(true);

        cfg.addResource(U.resolveIgniteUrl(hadoopCfg));

        final FileSystem fs = FileSystem.get(fsPath.toUri(), cfg);

        Path workDir = new Path(fsPath, "/fsload");

        fs.delete(workDir, true);

        fs.mkdirs(workDir, FsPermission.getDefault());

        final Path[] dirs;

        if (primaryOnly)
            dirs = new Path[]{mkdir(fs, workDir, DIR_PRIMARY_MODE)};
        else
            dirs = new Path[]{mkdir(fs, workDir, DIR_PRIMARY_MODE), mkdir(fs, workDir, DIR_PROXY_MODE),
                mkdir(fs, workDir, DIR_DUAL_SYNC_MODE), mkdir(fs, workDir, DIR_DUAL_ASYNC_MODE)};

        try {
            ExecutorService exec = Executors.newFixedThreadPool(threads);

            Collection<Future<?>> futs = new ArrayList<>(threads);

            for (int i = 0; i < threads; i++) {
                final int filesPerThread;

                if (i == 0 && files % threads != 0)
                    filesPerThread = files / threads + files % threads;
                else
                    filesPerThread = files / threads;

                futs.add(exec.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        runLoad(fs, dirs, filesPerThread, reads, writes, deletes, minSize, maxSize, delay);

                        return null;
                    }
                }));
            }

            exec.shutdown();

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    X.error("Error during execution: " + e);

                    e.getCause().printStackTrace();
                }
            }
        }
        finally {
            try {
                fs.delete(workDir, true);
            }
            catch (IOException ignored) {
                // Ignore.
            }
        }
    }

    /**
     * Executes read/write/delete operations.
     *
     * @param fs File system.
     * @param dirs Directories where files should be created.
     * @param filesNum Files number.
     * @param reads Reads number.
     * @param writes Writes number.
     * @param deletes Deletes number.
     * @param minSize Min file size.
     * @param maxSize Max file size.
     * @param delay Delay between operations.
     * @throws Exception If some file system operation failed.
     */
    private void runLoad(FileSystem fs, Path[] dirs, int filesNum, int reads, int writes, int deletes,
        int minSize, int maxSize, long delay) throws Exception {
        Random random = random();

        List<T2<Path, Integer>> files = new ArrayList<>(filesNum);

        for (int i = 0; i < filesNum; i++) {
            int size = maxSize == minSize ? minSize : minSize + random.nextInt(maxSize - minSize);

            Path file = new Path(dirs[i % dirs.length], "file-" + UUID.randomUUID());

            createFile(fs, file, size, CREATE_BUF_SIZE);

            files.add(new T2<>(file, size));
        }

        List<Path> toDel = new ArrayList<>(deletes);

        for (int i = 0; i < deletes; i++) {
            int size = maxSize == minSize ? minSize : minSize + random.nextInt(maxSize - minSize);

            Path file = new Path(dirs[i % dirs.length], "file-to-delete-" + UUID.randomUUID());

            createFile(fs, file, size, CREATE_BUF_SIZE);

            toDel.add(file);
        }

        while (reads > 0 || writes > 0 || deletes > 0) {
            if (reads > 0) {
                reads--;

                T2<Path, Integer> file = files.get(reads % files.size());

                readFull(fs, file.get1(), CREATE_BUF_SIZE);

                int fileSize = file.get2();

                readRandom(fs, file.get1(), fileSize, random.nextInt(fileSize) + 1);
            }

            if (writes > 0) {
                writes--;

                T2<Path, Integer> file = files.get(writes % files.size());

                overwriteFile(fs, file.get1(), file.get2(), CREATE_BUF_SIZE);

                appendToFile(fs, file.get1(), random.nextInt(CREATE_BUF_SIZE) + 1);
            }

            if (deletes > 0) {
                deletes--;

                deleteFile(fs, toDel.get(deletes));
            }

            U.sleep(delay);
        }
    }

    /**
     * Creates file.
     *
     * @param fs File system.
     * @param file File path.
     * @param fileSize File size.
     * @param bufSize Write buffer size.
     * @throws IOException If operation failed.
     */
    private static void createFile(FileSystem fs, Path file, int fileSize, int bufSize) throws IOException {
        create(fs, file, fileSize, bufSize, false);
    }

    /**
     * Overwrites file.
     *
     * @param fs File system.
     * @param file File path.
     * @param fileSize File size.
     * @param bufSize Write buffer size.
     * @throws IOException If operation failed.
     */
    private static void overwriteFile(FileSystem fs, Path file, int fileSize, int bufSize) throws IOException {
        create(fs, file, fileSize, bufSize, true);
    }

    /**
     * Appends to file.
     *
     * @param fs File system.
     * @param file File path.
     * @param appendSize Append size.
     * @throws IOException If operation failed.
     */
    private static void appendToFile(FileSystem fs, Path file, int appendSize) throws IOException {
        try (FSDataOutputStream out = fs.append(file)) {
            out.write(new byte[appendSize]);
        }
    }

    /**
     * Reads whole file.
     *
     * @param fs File system.
     * @param file File path.
     * @param bufSize Read buffer size.
     * @throws IOException If operation failed.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private static void readFull(FileSystem fs, Path file, int bufSize) throws IOException {
        try (FSDataInputStream in = fs.open(file)) {
            byte[] readBuf = new byte[bufSize];

            while (in.read(readBuf) > 0) {
                // No-op.
            }
        }
    }

    /**
     * Deletes file.
     *
     * @param fs File system.
     * @param path File path.
     * @throws IOException If operation failed.
     */
    private static void deleteFile(FileSystem fs, Path path) throws IOException {
        fs.delete(path, false);
    }

    /**
     * Reads from random position.
     *
     * @param fs File system.
     * @param path File path.
     * @param fileSize File size.
     * @param readSize Read size.
     * @throws IOException If operation failed.
     */
    private static void readRandom(FileSystem fs, Path path, int fileSize, int readSize) throws IOException {
        byte[] readBuf = new byte[readSize];

        try (FSDataInputStream in = fs.open(path)) {
            in.seek(random().nextInt(fileSize));

            in.read(readBuf);
        }
    }

    /**
     * Creates file.
     *
     * @param fs File system.
     * @param file File path.
     * @param fileSize File size.
     * @param bufSize Buffer size.
     * @param overwrite Overwrite flag.
     * @throws IOException If operation failed.
     */
    private static void create(FileSystem fs, Path file, int fileSize, int bufSize, boolean overwrite)
        throws IOException {
        try (FSDataOutputStream out = fs.create(file, overwrite)) {
            int size = 0;

            byte[] buf = new byte[bufSize];

            while (size < fileSize) {
                int len = Math.min(fileSize - size, bufSize);

                out.write(buf, 0, len);

                size += len;
            }
        }
    }

    /**
     * Creates directory in the given parent directory.
     *
     * @param fs File system.
     * @param parentDir Parent directory.
     * @param dirName Directory name.
     * @return Path for created directory.
     * @throws IOException If operation failed.
     */
    private static Path mkdir(FileSystem fs, Path parentDir, String dirName) throws IOException {
        Path path = new Path(parentDir, dirName);

        fs.mkdirs(path, FsPermission.getDefault());

        return path;
    }

    /**
     * @return Thread local random.
     */
    private static Random random() {
        return ThreadLocalRandom.current();
    }
}
