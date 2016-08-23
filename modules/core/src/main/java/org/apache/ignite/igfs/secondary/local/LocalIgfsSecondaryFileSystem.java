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

package org.apache.ignite.igfs.secondary.local;

import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathIsNotDirectoryException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemIgfsFile;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalIgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Secondary file system which delegates to local file system.
 */
public class LocalIgfsSecondaryFileSystem implements IgfsSecondaryFileSystem, LifecycleAware {
    /** Default buffer size. */
    private static final int DFLT_BUF_SIZE = 8 * 1024;

    /** Path that will be added to each passed path. */
    private String workDir;

    /**
     * Heuristically checks if exception was caused by invalid HDFS version and returns appropriate exception.
     *
     * @param e Exception to check.
     * @param msg Detailed error message.
     * @return Appropriate exception.
     */
    private IgfsException handleSecondaryFsError(IOException e, String msg) {
        if (e instanceof FileNotFoundException)
            return new IgfsPathNotFoundException(e);
        else
            return new IgfsException(msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        return fileForPath(path).exists();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile update(IgfsPath path, Map<String, String> props) {
        throw new UnsupportedOperationException("Update operation is not yet supported.");
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) {
        File srcFile = fileForPath(src);
        File destFile = fileForPath(dest);

        if (!srcFile.exists())
            throw new IgfsPathNotFoundException("Failed to perform rename because source path not found: " + src);

        if (srcFile.isDirectory() && destFile.isFile())
            throw new IgfsPathIsNotDirectoryException("Failed to perform rename because destination path is " +
                "directory and source path is file [src=" + src + ", dest=" + dest + ']');

        try {
            if (destFile.isDirectory())
                Files.move(srcFile.toPath(), destFile.toPath().resolve(srcFile.getName()));
            else if(!srcFile.renameTo(destFile))
                throw new IgfsException("Failed to perform rename (underlying file system returned false) " +
                    "[src=" + src + ", dest=" + dest + ']');
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to rename [src=" + src + ", dest=" + dest + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        File f = fileForPath(path);

        if (!recursive || !f.isDirectory())
            return f.delete();
        else
            return deleteDirectory(f);
    }

    /**
     * Delete directory recursively.
     *
     * @param dir Directory.
     * @return {@code true} if successful.
     */
    private boolean deleteDirectory(File dir) {
        File[] entries = dir.listFiles();

        if (entries != null) {
            for (File entry : entries) {
                if (entry.isDirectory())
                    deleteDirectory(entry);
                else if (entry.isFile()) {
                    if (!entry.delete())
                        return false;
                }
                else
                    throw new UnsupportedOperationException("Symlink deletion is not yet supported: " + entry);
            }
        }

        return dir.delete();
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        if (!mkdirs0(fileForPath(path)))
            throw new IgniteException("Failed to make directories (underlying file system returned false): " + path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        mkdirs(path);
    }

    /**
     * Create directories.
     *
     * @param dir Directory.
     * @return Result.
     */
    private boolean mkdirs0(@Nullable File dir) {
        if (dir == null)
            return true; // Nothing to create.

        if (dir.exists())
            // Already exists, so no-op.
            return dir.isDirectory();
        else {
            File parentDir = dir.getParentFile();

            if (!mkdirs0(parentDir)) // Create parent first.
                return false;

            boolean res = dir.mkdir();

            if (!res)
                res = dir.exists(); // Tolerate concurrent creation.

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) {
        File[] entries = listFiles0(path);

        if (F.isEmpty(entries))
            return Collections.emptySet();
        else {
            Collection<IgfsPath> res = U.newHashSet(entries.length);

            for (File entry : entries)
                res.add(igfsPath(entry));

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) {
        File[] entries = listFiles0(path);

        if (F.isEmpty(entries))
            return Collections.emptySet();
        else {
            Collection<IgfsFile> res = U.newHashSet(entries.length);

            for (File entry : entries) {
                IgfsFile info = info(igfsPath(entry));

                if (info != null)
                    res.add(info);
            }

            return res;
        }
    }

    /**
     * Returns an array of File object. Under the specific path.
     *
     * @param path IGFS path.
     * @return Array of File objects.
     */
    @Nullable private File[] listFiles0(IgfsPath path) {
        File f = fileForPath(path);

        if (!f.exists())
            throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);
        else
            return f.listFiles();
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) {
        try {
            FileInputStream in = new FileInputStream(fileForPath(path));

            return new LocalIgfsSecondaryFileSystemPositionedReadable(in, bufSize);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to open file for read: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) {
        return create0(path, overwrite, DFLT_BUF_SIZE);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        return create0(path, overwrite, bufSize);
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        try {
            File file = fileForPath(path);

            boolean exists = file.exists();

            if (exists)
                return new BufferedOutputStream(new FileOutputStream(file, true), bufSize);
            else {
                if (create)
                    return create0(path, false, bufSize);
                else
                    throw new IgfsPathNotFoundException("Failed to append to file because it doesn't exist: " + path);
            }
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to append to file because it doesn't exist: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(final IgfsPath path) {
        File f = fileForPath(path);

        if (!f.exists())
            return null;

        boolean isDir = f.isDirectory();

        if (isDir)
            return new LocalFileSystemIgfsFile(path, false, true, 0, f.lastModified(), 0, null);
        else
            return new LocalFileSystemIgfsFile(path, f.isFile(), false, 0, f.lastModified(), f.length(), null);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        throw new UnsupportedOperationException("usedSpaceSize operation is not yet supported.");
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (workDir != null)
            workDir = new File(workDir).getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /**
     * Get work directory.
     *
     * @return Work directory.
     */
    @Nullable public String getWorkDirectory() {
        return workDir;
    }

    /**
     * Set work directory.
     *
     * @param workDir Work directory.
     */
    public void setWorkDirectory(@Nullable String workDir) {
        this.workDir = workDir;
    }

    /**
     * Create file for IGFS path.
     *
     * @param path IGFS path.
     * @return File object.
     */
    private File fileForPath(IgfsPath path) {
        if (workDir == null)
            return new File(path.toString());
        else {
            if ("/".equals(path.toString()))
                return new File(workDir);
            else
                return new File(workDir, path.toString());
        }
    }

    /**
     * Create IGFS path for file.
     *
     * @param f File object.
     * @return IFGS path.
     * @throws IgfsException If failed.
     */
    private IgfsPath igfsPath(File f) throws IgfsException {
        String path = f.getAbsolutePath();

        if (workDir != null) {
            if (!path.startsWith(workDir))
                throw new IgfsException("Path is not located in the work directory [workDir=" + workDir +
                    ", path=" + path + ']');

            path = path.substring(workDir.length(), path.length());
        }

        return new IgfsPath(path);
    }

    /**
     * Internal create routine.
     *
     * @param path Path.
     * @param overwrite Overwirte flag.
     * @param bufSize Buffer size.
     * @return Output stream.
     */
    private OutputStream create0(IgfsPath path, boolean overwrite, int bufSize) {
        File file = fileForPath(path);

        boolean exists = file.exists();

        if (exists) {
            if (!overwrite)
                throw new IgfsPathAlreadyExistsException("Failed to create a file because it already exists: " + path);
        }
        else {
            File parent = file.getParentFile();

            if (!mkdirs0(parent))
                throw new IgfsException("Failed to create parent directory for file (underlying file system " +
                    "returned false): " + path);
        }

        try {
            return new BufferedOutputStream(new FileOutputStream(file), bufSize);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to create file [path=" + path + ", overwrite=" + overwrite + ']');
        }
    }
}