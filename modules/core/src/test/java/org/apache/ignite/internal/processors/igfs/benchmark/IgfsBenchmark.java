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

package org.apache.ignite.internal.processors.igfs.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;

/**
 *
 */
class FileOperation {
    /** Buff size. */
    public static final int BUFF_SIZE = 8192;

    /** Data bufer. */
    ByteBuffer dataBufer = ByteBuffer.allocate(BUFF_SIZE);

    /** Filesystem. */
    protected final IgniteFileSystem fs;

    /**
     * @param fs Ignite filesystem to benchmark.
     */
    public FileOperation(IgniteFileSystem fs) {
        this.fs = fs;
    }

    /**
     * @param path Path to do operation.
     * @throws Exception If failed.
     */
    public void handleFile(String path) throws Exception {
        // No-op.
    }

    /**
     * @param path Path to do operation.
     * @throws Exception If failed.
     */
    public void preHandleDir(String path) throws Exception {
        // No-op.
    }

    /**
     * @param path Path to do operation.
     * @throws Exception If failed.
     */
    public void postHandleDir(String path) throws Exception {
        // No-op.
    }
}

/**
 *
 */
class WriteFileOperation extends FileOperation {
    /** Size. */
    private int size;

    /**
     * @param fs Filesystem/
     * @param size Size to write.
     */
    public WriteFileOperation(IgniteFileSystem fs, int size) {
        super(fs);
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public void handleFile(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        IgfsOutputStream out;

        try {
            out = fs.create(path, false);
        }
        catch (IgniteException ex) {
            System.out.println("create file " + path.toString() + " failed: " + ex);
            throw ex;
        }

        try {
            for (int i = 0; i < size / dataBufer.capacity(); i++)
                out.write(dataBufer.array());
        }
        catch (IOException ex) {
            System.out.println("write file " + path.toString() + " failed: " + ex);
            throw ex;
        }
        finally {
            out.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void preHandleDir(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);

        if (fs.exists(path))
            throw new IgniteException("path " + path.toString() + " already exists");

        try {
            fs.mkdirs(path);
        }
        catch (IgniteException ex) {
            throw ex;
        }
    }
}

/**
 *
 */
class ReadFileOperation extends FileOperation {
    /** Size. */
    private int size;

    /**
     * @param fs Filesystem
     * @param size Size to read.
     */
    public ReadFileOperation(IgniteFileSystem fs, int size) {
        super(fs);
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public void handleFile(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        IgfsInputStream in;

        try {
            in = fs.open(path);
        }
        catch (IgfsPathNotFoundException ex) {
            System.out.println("file " + path.toString() + " not exist: " + ex);
            throw ex;
        }
        catch (IgniteException ex) {
            System.out.println("open file " + path.toString() + " failed: " + ex);
            throw ex;
        }

        try {
            for (int i = 0; i < size / dataBufer.capacity(); i++)
                in.read(dataBufer.array());
        }
        catch (IOException ex) {
            System.out.println("read file " + path.toString() + " failed: " + ex);
            throw ex;
        }
        finally {
            in.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void preHandleDir(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);

        if (!fs.exists(path)) {
            System.out.println("path " + path.toString() + " not exist");
            throw new IgniteException("path " + path.toString() + " not exist");
        }
    }
}

/**
 *
 */
class DeleteFileOperation extends FileOperation {
    /** Size. */
    private int size;

    /**
     * @param fs Filesystem.
     * @param size Size.
     */
    public DeleteFileOperation(IgniteFileSystem fs, int size) {
        super(fs);
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public void handleFile(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        fs.delete(path, false);
    }

    /** {@inheritDoc} */
    @Override public void postHandleDir(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        fs.delete(path, false);
    }
}

/**
 *
 */
class InfoFileOperation extends FileOperation {
    /**
     * @param fs Filesystem.
     */
    public InfoFileOperation(IgniteFileSystem fs) {
        super(fs);
    }

    /** {@inheritDoc} */
    @Override public void handleFile(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        IgfsFile info = fs.info(path);

        assert info != null : "Info must be not null for exists file. All files must be exists for benchmark";
    }

    /** {@inheritDoc} */
    @Override public void postHandleDir(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);
        IgfsFile info = fs.info(path);

        assert info != null : "Info must be not null for exists dir. All dirs must be exists for benchmark";
    }
}

/**
 *
 */
class ListPathFileOperation extends FileOperation {
    /**
     * @param fs Filesystem.
     */
    public ListPathFileOperation(IgniteFileSystem fs) {
        super(fs);
    }

    /** {@inheritDoc} */
    @Override public void postHandleDir(String strPath) throws Exception {
        IgfsPath path = new IgfsPath(strPath);

        Collection<IgfsPath> lst = fs.listPaths(path);

        assert lst != null : "List of paths must not be null";
    }
}

/**
 *
 */
public class IgfsBenchmark {
    /** Path. */
    private final String path;

    /** Depth. */
    private final int depth;

    /** Width. */
    private final int subDirsCount;

    /** Count. */
    private final int filesCount;

    /** Size. */
    private final int size;

    /**
     * @param path Root test path.
     * @param depth Directory depth.
     * @param subDirsCount Count of subdirectories.
     * @param filesCount Count of files.
     * @param size Size of file.
     */
    public IgfsBenchmark(String path,
        int depth,
        int subDirsCount,
        int filesCount,
        int size) {
        this.path = path;
        this.depth = depth;
        this.subDirsCount = subDirsCount;
        this.filesCount = filesCount;
        this.size = (size > FileOperation.BUFF_SIZE) ? size : FileOperation.BUFF_SIZE;
    }

    /**
     * @param lst List of measurement results.
     * @return Average value.
     */
    public static long avg(List<Long> lst) {
        if (lst.isEmpty())
            throw new IllegalArgumentException("List must be not empty");

        long sum = 0;
        for (long l : lst)
            sum += l;

        return sum / lst.size();
    }

    /**
     * @param lst List of measurement results.
     * @param avg Average value.
     * @return THe value of the standard derivation.
     */
    public static long stdDev(List<Long> lst, long avg) {
        if (lst.isEmpty())
            throw new IllegalArgumentException("List must be not empty");

        long sum = 0;
        for (long l : lst)
            sum += (l - avg) * (l - avg);

        return (long)Math.sqrt((double)sum / (double)lst.size());
    }

    /**
     * @param args Commandline arguments
     */
    public static void main(String[] args) {
        Ignition.setClientMode(Boolean.getBoolean("clientMode"));

        Ignite ignite = Ignition.start(System.getProperty("cfg", "default-config.xml"));

        int wormUpCount = Integer.getInteger("wormup", 2);
        int cycles = Integer.getInteger("cycles", 10);

        final IgfsBenchmark fsTest = new IgfsBenchmark(
            System.getProperty("testDir", "/test"),
            Integer.getInteger("depth", 3),
            Integer.getInteger("subDirs", 10),
            Integer.getInteger("files", 10),
            Integer.getInteger("fileSize", 8) * 1024);

        final IgniteFileSystem fs = ignite.fileSystem("igfs");

        try {
            for (int i = 0; i < wormUpCount; ++i) {
                System.out.println("Wormup #" + i + " / " + wormUpCount);
                fsTest.testWriteFile(fs);
                fsTest.testReadFile(fs);
                fsTest.testDeleteFile(fs);
            }
        }
        catch (Exception ex) {
            System.err.println("Wormup error");
            ex.printStackTrace(System.err);
            Ignition.stop(false);
            return;
        }

        List<Long> writeRes = new ArrayList<>(cycles);
        List<Long> readRes = new ArrayList<>(cycles);
        List<Long> infoRes = new ArrayList<>(cycles);
        List<Long> listRes = new ArrayList<>(cycles);
        List<Long> delRes = new ArrayList<>(cycles);

        try {
            for (int i = 0; i < cycles; ++i) {
                System.out.println("Benchmark cycle #" + i + " / " + cycles);

                writeRes.add(bench(new Runnable() {
                    @Override public void run() {
                        fsTest.testWriteFile(fs);
                    }
                }));

                readRes.add(bench(new Runnable() {
                    @Override public void run() {
                        fsTest.testReadFile(fs);
                    }
                }));

                infoRes.add(bench(new Runnable() {
                    @Override public void run() {
                        fsTest.testInfoFile(fs);
                    }
                }));

                listRes.add(bench(new Runnable() {
                    @Override public void run() {
                        fsTest.testListPathFile(fs);
                    }
                }));

                delRes.add(bench(new Runnable() {
                    @Override public void run() {
                        fsTest.testDeleteFile(fs);
                    }
                }));
            }

            System.out.println("\n");
            System.out.println("Write " + avg(writeRes) + " +/- " + stdDev(writeRes, avg(writeRes)));
            System.out.println("Read " + avg(readRes) + " +/- " + stdDev(readRes, avg(readRes)));
            System.out.println("Info " + avg(infoRes) + " +/- " + stdDev(infoRes, avg(infoRes)));
            System.out.println("List " + avg(listRes) + " +/- " + stdDev(listRes, avg(listRes)));
            System.out.println("Delete " + avg(delRes) + " +/- " + stdDev(delRes, avg(delRes)));
        }
        catch (Exception ex) {
            System.err.println("Benchmark error");
            ex.printStackTrace(System.err);
        }
        finally {
            Ignition.stop(false);
        }
    }

    /**
     * @param parentPath Begin path.
     * @param depth Current deep.
     * @return List of subdirs.
     */
    private String[] buildPath(String parentPath, int depth) {
        String curPath[] = new String[subDirsCount];

        for (int i = 1; i <= curPath.length; i++)
            curPath[i - 1] = parentPath + "/vdb." + depth + "_" + i + ".dir";

        return curPath;
    }

    /**
     * @param parentPath Begin path.
     * @param operation Test operation to do.
     * @throws Exception If failed.
     */
    private void recurseFile(String parentPath, FileOperation operation) throws Exception {
        for (int i = 1; i <= filesCount; i++) {
            String filePath = parentPath + "/vdb_f" + String.format("%0" + String.valueOf(this.filesCount).length() + "d", i) + ".file";
            operation.handleFile(filePath);
        }
    }

    /**
     * @param parentPath Begin path.
     * @param depth depth of recurse.
     * @param operation Test operation to do.
     * @throws Exception If failed.
     */
    private void recursePath(String parentPath, int depth, FileOperation operation) throws Exception {
        if (depth == this.depth + 1)
            recurseFile(parentPath, operation);
        else {
            String curPath[] = buildPath(parentPath, depth);

            for (String path : curPath) {
                operation.preHandleDir(path);
                recursePath(path, depth + 1, operation);
                operation.postHandleDir(path);
            }
        }
    }

    /**
     * Do read file operations. Files must be exist.
     *
     * @param fs Filesystem.
     */
    public void testReadFile(IgniteFileSystem fs) {
        try {
            recursePath(path, 1, new ReadFileOperation(fs, size));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Do write file operations.
     *
     * @param fs Filesystem.
     */
    public void testWriteFile(IgniteFileSystem fs) {
        try {
            recursePath(path, 1, new WriteFileOperation(fs, size));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Do delete file operations. Files must be exist.
     *
     * @param fs Filesystem.
     */
    public void testDeleteFile(IgniteFileSystem fs) {
        try {
            recursePath(path, 1, new DeleteFileOperation(fs, 0));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Do info file operations. Files must be exist.
     *
     * @param fs Filesystem.
     */
    public void testInfoFile(IgniteFileSystem fs) {
        try {
            recursePath(path, 1, new InfoFileOperation(fs));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Do info file operations. Files must be exist.
     *
     * @param fs Filesystem.
     */
    public void testListPathFile(IgniteFileSystem fs) {
        try {
            recursePath(path, 1, new ListPathFileOperation(fs));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param r Runnable.
     * @return Time of execution in millis.
     */
    public static long bench(Runnable r) {
        long t0 = System.currentTimeMillis();

        r.run();

        return System.currentTimeMillis() - t0;
    }
}
