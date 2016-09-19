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

package org.apache.ignite.internal.processors.hadoop.fs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Local file system implementation for Hadoop.
 */
public class HadoopRawLocalFileSystem extends FileSystem {
    /** Working directory for each thread. */
    private final ThreadLocal<Path> workDir = new ThreadLocal<Path>() {
        @Override protected Path initialValue() {
            return getInitialWorkingDirectory();
        }
    };

    /**
     * Converts Hadoop path to local path.
     *
     * @param path Hadoop path.
     * @return Local path.
     */
    File convert(Path path) {
        checkPath(path);

        if (path.isAbsolute())
            return new File(path.toUri().getPath());

        return new File(getWorkingDirectory().toUri().getPath(), path.toUri().getPath());
    }

    /** {@inheritDoc} */
    @Override public Path getHomeDirectory() {
        return makeQualified(new Path(System.getProperty("user.home")));
    }

    /** {@inheritDoc} */
    @Override public Path getInitialWorkingDirectory() {
        File f = new File(System.getProperty("user.dir"));

        return new Path(f.getAbsoluteFile().toURI()).makeQualified(getUri(), null);
    }

    /** {@inheritDoc} */
    @Override public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);

        setConf(conf);

        String initWorkDir = conf.get(HadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP);

        if (initWorkDir != null)
            setWorkingDirectory(new Path(initWorkDir));
    }

    /** {@inheritDoc} */
    @Override public URI getUri() {
        return FsConstants.LOCAL_FS_URI;
    }

    /** {@inheritDoc} */
    @Override public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(new InStream(checkExists(convert(f))));
    }

    /** {@inheritDoc} */
    @Override public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufSize,
        short replication, long blockSize, Progressable progress) throws IOException {
        File file = convert(f);

        if (!overwrite && !file.createNewFile())
            throw new IOException("Failed to create new file: " + f.toUri());

        return out(file, false, bufSize);
    }

    /**
     * @param file File.
     * @param append Append flag.
     * @return Output stream.
     * @throws IOException If failed.
     */
    private FSDataOutputStream out(File file, boolean append, int bufSize) throws IOException {
        return new FSDataOutputStream(new BufferedOutputStream(new FileOutputStream(file, append),
            bufSize < 32 * 1024 ? 32 * 1024 : bufSize), new Statistics(getUri().getScheme()));
    }

    /** {@inheritDoc} */
    @Override public FSDataOutputStream append(Path f, int bufSize, Progressable progress) throws IOException {
        return out(convert(f), true, bufSize);
    }

    /** {@inheritDoc} */
    @Override public boolean rename(Path src, Path dst) throws IOException {
        return convert(src).renameTo(convert(dst));
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
        File file = convert(f);

        if (file.isDirectory() && !recursive)
            throw new IOException("Failed to remove directory in non recursive mode: " + f.toUri());

        return U.delete(file);
    }

    /** {@inheritDoc} */
    @Override public void setWorkingDirectory(Path dir) {
        workDir.set(fixRelativePart(dir));

        checkPath(dir);
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workDir.get();
    }

    /** {@inheritDoc} */
    @Override public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        if(f == null)
            throw new IllegalArgumentException("mkdirs path arg is null");

        Path parent = f.getParent();

        File p2f = convert(f);

        if(parent != null) {
            File parent2f = convert(parent);

            if(parent2f != null && parent2f.exists() && !parent2f.isDirectory())
                throw new FileAlreadyExistsException("Parent path is not a directory: " + parent);

        }

        return (parent == null || mkdirs(parent)) && (p2f.mkdir() || p2f.isDirectory());
    }

    /** {@inheritDoc} */
    @Override public FileStatus getFileStatus(Path f) throws IOException {
        return fileStatus(checkExists(convert(f)));
    }

    /**
     * @return File status.
     */
    private FileStatus fileStatus(File file) throws IOException {
        boolean dir = file.isDirectory();

        java.nio.file.Path path = dir ? null : file.toPath();

        return new FileStatus(dir ? 0 : file.length(), dir, 1, 4 * 1024, file.lastModified(), file.lastModified(),
            /*permission*/null, /*owner*/null, /*group*/null, dir ? null : Files.isSymbolicLink(path) ?
            new Path(Files.readSymbolicLink(path).toUri()) : null, new Path(file.toURI()));
    }

    /**
     * @param file File.
     * @return Same file.
     * @throws FileNotFoundException If does not exist.
     */
    private static File checkExists(File file) throws FileNotFoundException {
        if (!file.exists())
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " does not exist.");

        return file;
    }

    /** {@inheritDoc} */
    @Override public FileStatus[] listStatus(Path f) throws IOException {
        File file = convert(f);

        if (checkExists(file).isFile())
            return new FileStatus[] {fileStatus(file)};

        File[] files = file.listFiles();

        FileStatus[] res = new FileStatus[files.length];

        for (int i = 0; i < res.length; i++)
            res[i] = fileStatus(files[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSymlinks() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
        Files.createSymbolicLink(convert(link).toPath(), convert(target).toPath());
    }

    /** {@inheritDoc} */
    @Override public FileStatus getFileLinkStatus(Path f) throws IOException {
        return getFileStatus(getLinkTarget(f));
    }

    /** {@inheritDoc} */
    @Override public Path getLinkTarget(Path f) throws IOException {
        File file = Files.readSymbolicLink(convert(f).toPath()).toFile();

        return new Path(file.toURI());
    }

    /**
     * Input stream.
     */
    private static class InStream extends InputStream implements Seekable, PositionedReadable {
        /** */
        private final RandomAccessFile file;

        /**
         * @param f File.
         * @throws IOException If failed.
         */
        public InStream(File f) throws IOException {
            file = new RandomAccessFile(f, "r");
        }

        /** {@inheritDoc} */
        @Override public synchronized int read() throws IOException {
            return file.read();
        }

        /** {@inheritDoc} */
        @Override public synchronized int read(byte[] b, int off, int len) throws IOException {
            return file.read(b, off, len);
        }

        /** {@inheritDoc} */
        @Override public synchronized void close() throws IOException {
            file.close();
        }

        /** {@inheritDoc} */
        @Override public synchronized int read(long pos, byte[] buf, int off, int len) throws IOException {
            long pos0 = file.getFilePointer();

            file.seek(pos);
            int res = file.read(buf, off, len);

            file.seek(pos0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public void readFully(long pos, byte[] buf, int off, int len) throws IOException {
            if (read(pos, buf, off, len) != len)
                throw new IOException();
        }

        /** {@inheritDoc} */
        @Override public void readFully(long pos, byte[] buf) throws IOException {
            readFully(pos, buf, 0, buf.length);
        }

        /** {@inheritDoc} */
        @Override public synchronized void seek(long pos) throws IOException {
            file.seek(pos);
        }

        /** {@inheritDoc} */
        @Override public synchronized long getPos() throws IOException {
            return file.getFilePointer();
        }

        /** {@inheritDoc} */
        @Override public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }
    }
}