/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop.fs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;

/**
 * Local file system implementation for Hadoop.
 */
public class GridHadoopRawLocalFileSystem extends FileSystem {
    /** */
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
            return new File(path.toUri());

        return new File(getWorkingDirectory().toUri().getPath(), path.toUri().getPath());
    }

    /** {@inheritDoc} */
    @Override protected Path getInitialWorkingDirectory() {
        return makeQualified(new Path(System.getProperty("user.dir")));
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
        workDir.set(dir);
        checkPath(dir);
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workDir.get();
    }

    /** {@inheritDoc} */
    @Override public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return convert(f).mkdirs();
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
