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

package org.apache.ignite.hadoop.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProperties;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.io.GridFilenameUtils;
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
import java.net.URI;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Secondary file system which delegates to local file system.
 */
public class LocalIgfsSecondaryFileSystem implements IgfsSecondaryFileSystem, LifecycleAware {
    /** Default buffer size. */
    // TODO: IGNITE-3643.
    public static final int DFLT_BUF_SIZE = 8 * 1024;

    /** The default user name. It is used if no user context is set. */
    private String dfltUsrName = IgfsUtils.fixUserName(null);

    /** Factory. */
    private HadoopFileSystemFactory fsFactory;

    /** Path that will be added to each passed path. */
    private String workDir;

    /** Buffer size. */
    private int bufSize = DFLT_BUF_SIZE;

    /**
     * Default constructor.
     */
    public LocalIgfsSecondaryFileSystem() {
        CachingHadoopFileSystemFactory fsFactory0 = new CachingHadoopFileSystemFactory();

        fsFactory0.setUri("file:///");

        fsFactory = fsFactory0;
    }

    /**
     * Convert IGFS path into Hadoop path.
     *
     * @param path IGFS path.
     * @return Hadoop path.
     */
    private Path convert(IgfsPath path) {
        URI uri = fileSystemForUser().getUri();

        return new Path(uri.getScheme(), uri.getAuthority(), addParent(path.toString()));
    }

    /**
     * @param path Path to which parrent should be added.
     * @return Path with added root.
     */
    private String addParent(String path) {
        if (path.startsWith("/"))
            path = path.substring(1, path.length());

        if (workDir == null)
            return path;
        else
            return GridFilenameUtils.concat(workDir, path);
    }

    /**
     * Heuristically checks if exception was caused by invalid HDFS version and returns appropriate exception.
     *
     * @param e Exception to check.
     * @param detailMsg Detailed error message.
     * @return Appropriate exception.
     */
    private IgfsException handleSecondaryFsError(IOException e, String detailMsg) {
        return cast(detailMsg, e);
    }

    /**
     * Cast IO exception to IGFS exception.
     *
     * @param msg Error message.
     * @param e IO exception.
     * @return IGFS exception.
     */
    public static IgfsException cast(String msg, IOException e) {
        if (e instanceof FileNotFoundException)
            return new IgfsPathNotFoundException(e);
        else if (e instanceof ParentNotDirectoryException)
            return new IgfsParentNotDirectoryException(msg, e);
        else if (e instanceof PathIsNotEmptyDirectoryException)
            return new IgfsDirectoryNotEmptyException(e);
        else if (e instanceof PathExistsException)
            return new IgfsPathAlreadyExistsException(msg, e);
        else
            return new IgfsException(msg, e);
    }

    /**
     * Convert Hadoop FileStatus properties to map.
     *
     * @param status File status.
     * @return IGFS attributes.
     */
    private static Map<String, String> properties(FileStatus status) {
        FsPermission perm = status.getPermission();

        if (perm == null)
            perm = FsPermission.getDefault();

        HashMap<String, String> res = new HashMap<>(3);

        res.put(IgfsUtils.PROP_PERMISSION, String.format("%04o", perm.toShort()));
        res.put(IgfsUtils.PROP_USER_NAME, status.getOwner());
        res.put(IgfsUtils.PROP_GROUP_NAME, status.getGroup());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        return fileForPath(path).exists();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile update(IgfsPath path, Map<String, String> props) {
        // TODO: IGNITE-3645.
        HadoopIgfsProperties props0 = new HadoopIgfsProperties(props);

        final FileSystem fileSys = fileSystemForUser();

        try {
            if (props0.userName() != null || props0.groupName() != null)
                fileSys.setOwner(convert(path), props0.userName(), props0.groupName());

            if (props0.permission() != null)
                fileSys.setPermission(convert(path), props0.permission());
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to update file properties [path=" + path + "]");
        }

        //Result is not used in case of secondary FS.
        return null;
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) {
        try {
            File srcFile = fileForPath(src);
            File destFile = fileForPath(dest);

            if (!srcFile.exists())
                throw new IOException("File not found: " + srcFile);

            if (srcFile.isDirectory() && destFile.isFile())
                throw new IOException("Failed rename directory to existing file: [src=" + src + ", dest=" + dest + ']');

            if (destFile.isDirectory())
                Files.move(srcFile.toPath(), destFile.toPath().resolve(srcFile.getName()));
            else if(!srcFile.renameTo(destFile))
                throw new IgfsException("Failed to rename (secondary file system returned false) " +
                    "[src=" + src + ", dest=" + dest + ']');
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to rename [src=" + src + ", dest="+ dest + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        try {
            File f = fileForPath(path);

            // TODO: IGNITE-3642.
            if (!recursive || !f.isDirectory())
                return f.delete();
            else
                return deleteDirectory(f);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to delete file [path=" + path + ", recursive=" + recursive + "]");
        }
    }

    /**
     * Delete directory recursively.
     *
     * @param dir Directory.
     * @throws IOException If fails.
     * @return {@code true} if successful.
     */
    private boolean deleteDirectory(File dir) throws IOException {
        File[] entries = dir.listFiles();

        if (entries != null) {
            for (File entry : entries) {
                if (entry.isDirectory())
                    deleteDirectory(entry);
                else if (entry.isFile()) {
                    if (!entry.delete())
                        throw new IOException("Cannot remove file: " + entry);
                }
                else
                    // TODO: IGNITE-3642.
                    throw new UnsupportedOperationException("Symlink deletion is not supported: " + entry);
            }
        }

        if (!dir.delete())
            throw new IOException("Cannot remove directory: " + dir);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        if (!mkdirs0(fileForPath(path)))
            throw new IgniteException("Failed to make directories [path=" + path + "]");
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        // TODO: IGNITE-3641.
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

        if (dir.exists()) {
            if (dir.isDirectory())
                return true; // Already exists, so no-op.
            else
                // TODO: IGNITE-3646.
                return false;
        }
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
        return create0(path, overwrite, bufSize);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        // TODO: IGNITE-3648.
        return create0(path, overwrite, bufSize);
    }
    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        // TODO: IGNITE-3648.
        try {
            File file = fileForPath(path);

            boolean exists = file.exists();

            if (exists)
                return new BufferedOutputStream(new FileOutputStream(file, true), bufSize);
            else {
                if (create)
                    return create0(path, false, bufSize);
                else
                    throw new IOException("File not found: " + path);
            }
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to append file [path=" + path + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(final IgfsPath path) {
        try {
            // TODO: IGNITE-3650.
            final FileStatus status = fileSystemForUser().getFileStatus(convert(path));

            if (status == null)
                return null;

            final Map<String, String> props = properties(status);

            return new IgfsFile() {
                @Override public IgfsPath path() {
                    return path;
                }

                @Override public boolean isFile() {
                    return status.isFile();
                }

                @Override public boolean isDirectory() {
                    return status.isDirectory();
                }

                @Override public int blockSize() {
                    // By convention directory has blockSize == 0, while file has blockSize > 0:
                    return isDirectory() ? 0 : (int)status.getBlockSize();
                }

                @Override public long groupBlockSize() {
                    return status.getBlockSize();
                }

                @Override public long accessTime() {
                    return status.getAccessTime();
                }

                @Override public long modificationTime() {
                    return status.getModificationTime();
                }

                @Override public String property(String name) throws IllegalArgumentException {
                    String val = props.get(name);

                    if (val ==  null)
                        throw new IllegalArgumentException("File property not found [path=" + path + ", name=" + name + ']');

                    return val;
                }

                @Nullable @Override public String property(String name, @Nullable String dfltVal) {
                    String val = props.get(name);

                    return val == null ? dfltVal : val;
                }

                @Override public long length() {
                    return status.getLen();
                }

                /** {@inheritDoc} */
                @Override public Map<String, String> properties() {
                    return props;
                }
            };
        }
        catch (FileNotFoundException ignore) {
            return null;
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to get file status [path=" + path + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        try {
            // TODO: IGNITE-3651.
            // We don't use FileSystem#getUsed() since it counts only the files
            // in the filesystem root, not all the files recursively.
            return fileSystemForUser().getContentSummary(new Path("/")).getSpaceConsumed();
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to get used space size of file system.");
        }
    }

    /**
     * Gets the FileSystem for the current context user.
     * @return the FileSystem instance, never null.
     */
    private FileSystem fileSystemForUser() {
        String user = IgfsUserContext.currentUser();

        if (F.isEmpty(user))
            user = IgfsUtils.fixUserName(dfltUsrName);

        assert !F.isEmpty(user);

        try {
            return fsFactory.get(user);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (fsFactory == null)
            fsFactory = new CachingHadoopFileSystemFactory();

        if (fsFactory instanceof LifecycleAware)
            ((LifecycleAware) fsFactory).start();

        workDir = new File(workDir).getAbsolutePath();

        assert !workDir.endsWith("/") : workDir;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (fsFactory instanceof LifecycleAware)
             ((LifecycleAware)fsFactory).stop();
    }

    /**
     * Get work directory.
     *
     * @return Work directory.
     */
    public String getWorkDirectory() {
        // TODO: IGNITE-3652.
        return workDir;
    }

    /**
     * Set work directory.
     *
     * @param workDir Work directory.
     */
    public void setWorkDirectory(final String workDir) {
        this.workDir = workDir;
    }

    /**
     * Get buffer size.
     *
     * @return Buffer size.
     */
    public int getBufferSize() {
        return bufSize;
    }

    /**
     * Set buffer size.
     *
     * @param bufSize Buffer size.
     */
    public void setBufferSize(int bufSize) {
        this.bufSize = bufSize;
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
                    "path=" + path + ']');

            path = path.substring(workDir.length(), path.length());

            assert !path.startsWith("/") : "Path is not located in the work directory [workDir=" + workDir +
                "path=" + f + ']';
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
        try {
            File file = fileForPath(path);

            boolean exists = file.exists();

            if (exists) {
                if (!overwrite)
                    throw new IOException("File already exists.");
            }
            else {
                File parent = file.getParentFile();

                if (!mkdirs0(parent))
                    throw new IOException("Failed to create parent directory: " + parent);
            }

            return new BufferedOutputStream(new FileOutputStream(file), bufSize);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to create file [path=" + path + ", overwrite=" + overwrite + ']');
        }
    }
}