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

import java.util.ArrayList;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathIsNotDirectoryException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.igfs.IgfsDataManager;
import org.apache.ignite.internal.processors.igfs.IgfsImpl;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemBlockKey;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsBlockLocationImpl;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemIgfsFile;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemSizeVisitor;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemUtils;
import org.apache.ignite.internal.processors.igfs.secondary.local.LocalFileSystemPositionedReadable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.FileSystemResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Secondary file system which delegates to local file system.
 */
public class LocalIgfsSecondaryFileSystem implements IgfsSecondaryFileSystem, LifecycleAware {
    /** Path that will be added to each passed path. */
    private String workDir;

    /** Logger. */
    @SuppressWarnings("unused")
    @LoggerResource
    private IgniteLogger log;

    /** IGFS instance. */
    @SuppressWarnings("unused")
    @FileSystemResource
    private IgfsImpl igfs;

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
        File f = fileForPath(path);

        if (!f.exists())
            return null;

        updatePropertiesIfNeeded(path, props);

        return info(path);
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
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        File f = fileForPath(path);

        if (!recursive)
            return f.delete();
        else
            return deleteRecursive(f, false);
    }

    /**
     * Delete directory recursively.
     *
     * @param f Directory.
     * @param deleteIfExists Ignore delete errors if the file doesn't exist.
     * @return {@code true} if successful.
     */
    private boolean deleteRecursive(File f, boolean deleteIfExists) {
        BasicFileAttributes attrs;

        try {
            attrs = Files.readAttributes(f.toPath(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        }
        catch (IOException ignore) {
            return deleteIfExists && !f.exists();
        }

        if (!attrs.isDirectory() || attrs.isSymbolicLink())
            return f.delete() || (deleteIfExists && !f.exists());

        File[] entries = f.listFiles();

        if (entries != null) {
            for (File entry : entries) {
                boolean res = deleteRecursive(entry, true);

                if (!res)
                    return false;
            }
        }

        return f.delete() || (deleteIfExists && !f.exists());
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        if (!mkdirs0(fileForPath(path)))
            throw new IgniteException("Failed to make directories (underlying file system returned false): " + path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        mkdirs(path);

        updatePropertiesIfNeeded(path, props);
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

            return new LocalFileSystemPositionedReadable(in, bufSize);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to open file for read: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) {
        return create0(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        OutputStream os = create0(path, overwrite);

        try {
            updatePropertiesIfNeeded(path, props);

            return os;
        }
        catch (Exception err) {
            try {
                os.close();
            }
            catch (IOException closeErr) {
                err.addSuppressed(closeErr);
            }

            throw err;
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        try {
            File file = fileForPath(path);

            boolean exists = file.exists();

            if (exists) {
                OutputStream os = new FileOutputStream(file, true);

                try {
                    updatePropertiesIfNeeded(path, props);

                    return os;
                }
                catch (Exception err) {
                    try {
                        os.close();

                        throw err;
                    }
                    catch (IOException closeErr) {
                        err.addSuppressed(closeErr);

                        throw err;
                    }
                }
            }
            else {
                if (create)
                    return create(path, bufSize, false, 0, 0, props);
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
        File file = fileForPath(path);

        if (!file.exists())
            return null;

        boolean isDir = file.isDirectory();

        PosixFileAttributes attrs = LocalFileSystemUtils.posixAttributes(file);

        Map<String, String> props = LocalFileSystemUtils.posixAttributesToMap(attrs);

        BasicFileAttributes basicAttrs = LocalFileSystemUtils.basicAttributes(file);

        if (isDir) {
            return new LocalFileSystemIgfsFile(path, false, true, 0,
                basicAttrs.lastAccessTime().toMillis(), basicAttrs.lastModifiedTime().toMillis(), 0, props);
        }
        else {
            return new LocalFileSystemIgfsFile(path, file.isFile(), false, 0,
                basicAttrs.lastAccessTime().toMillis(), basicAttrs.lastModifiedTime().toMillis(), file.length(), props);
        }
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        Path p = fileForPath(IgfsPath.ROOT).toPath();

        try {
            LocalFileSystemSizeVisitor visitor = new LocalFileSystemSizeVisitor();

            Files.walkFileTree(p, visitor);

            return visitor.size();
        }
        catch (IOException e) {
            throw new IgfsException("Failed to calculate used space size.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long modificationTime, long accessTime) throws IgniteException {
        Path p = fileForPath(path).toPath();

        if (!Files.exists(p))
            throw new IgfsPathNotFoundException("Failed to set times (path not found): " + path);

        try {
            Files.getFileAttributeView(p, BasicFileAttributeView.class)
                .setTimes(
                    (modificationTime >= 0) ? FileTime.from(modificationTime, TimeUnit.MILLISECONDS) : null,
                    (accessTime >= 0) ? FileTime.from(accessTime, TimeUnit.MILLISECONDS) : null,
                    null);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to set times for path: " + path, e);
        }
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

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len,
        long maxLen) throws IgniteException {
        File f = fileForPath(path);

        if (!f.exists())
            throw new IgfsPathNotFoundException("File not found: " + path);

        // Create fake block & fake affinity for blocks
        long blockSize = igfs.configuration().getBlockSize();

        if (maxLen <= 0)
            maxLen = Long.MAX_VALUE;

        assert maxLen > 0 : "maxLen : " + maxLen;

        long end = start + len;

        Collection<IgfsBlockLocation> blocks = new ArrayList<>((int)(len / maxLen));

        IgfsDataManager data = igfs.context().data();

        Collection<ClusterNode> lastNodes = null;

        long lastBlockIdx = -1;

        IgfsBlockLocationImpl lastBlock = null;

        for (long offset = start; offset < end; ) {
            long blockIdx = offset / blockSize;

            // Each step is min of maxLen and end of block.
            long lenStep = Math.min(
                maxLen - (lastBlock != null ? lastBlock.length() : 0),
                (blockIdx + 1) * blockSize - offset);

            lenStep = Math.min(lenStep, end - offset);

            // Create fake affinity key to map blocks of secondary filesystem to nodes.
            LocalFileSystemBlockKey affKey = new LocalFileSystemBlockKey(path, blockIdx);

            if (blockIdx != lastBlockIdx) {
                Collection<ClusterNode> nodes = data.affinityNodes(affKey);

                if (!nodes.equals(lastNodes) && lastNodes != null && lastBlock != null) {
                    blocks.add(lastBlock);

                    lastBlock = null;
                }

                lastNodes = nodes;

                lastBlockIdx = blockIdx;
            }

            if(lastBlock == null)
                lastBlock = new IgfsBlockLocationImpl(offset, lenStep, lastNodes);
            else
                lastBlock.increaseLength(lenStep);

            if (lastBlock.length() == maxLen || lastBlock.start() + lastBlock.length() == end) {
                blocks.add(lastBlock);

                lastBlock = null;
            }

            offset += lenStep;
       }

        return blocks;
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
     * @param overwrite Overwrite flag.
     * @return Output stream.
     */
    private OutputStream create0(IgfsPath path, boolean overwrite) {
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
            return new FileOutputStream(file);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to create file [path=" + path + ", overwrite=" + overwrite + ']');
        }
    }

    /**
     * Update path properties if needed.
     *
     * @param path IGFS path
     * @param props Properties map.
     */
    private void updatePropertiesIfNeeded(IgfsPath path, Map<String, String> props) {
        if (props == null || props.isEmpty())
            return;

        File file = fileForPath(path);

        if (!file.exists())
            throw new IgfsPathNotFoundException("Failed to update properties for path: " + path);

        LocalFileSystemUtils.updateProperties(file, props.get(IgfsUtils.PROP_GROUP_NAME),
            props.get(IgfsUtils.PROP_PERMISSION));
    }
}
