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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
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
import org.apache.ignite.internal.processors.hadoop.HadoopPayloadAware;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProperties;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.igfs.IgfsFileImpl;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfo;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_GROUP_NAME;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_PERMISSION;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_USER_NAME;

/**
 * Secondary file system which delegates calls to an instance of Hadoop {@link FileSystem}.
 * <p>
 * Target {@code FileSystem}'s are created on per-user basis using passed {@link HadoopFileSystemFactory}.
 */
public class IgniteHadoopIgfsSecondaryFileSystem implements IgfsSecondaryFileSystem, LifecycleAware,
    HadoopPayloadAware {
    /** The default user name. It is used if no user context is set. */
    private String dfltUsrName;

    /** Factory. */
    private HadoopFileSystemFactory fsFactory;

    /**
     * Default constructor for Spring.
     */
    public IgniteHadoopIgfsSecondaryFileSystem() {
        // No-op.
    }

    /**
     * Simple constructor that is to be used by default.
     *
     * @param uri URI of file system.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(String uri) throws IgniteCheckedException {
        this(uri, null, null);
    }

    /**
     * Constructor.
     *
     * @param uri URI of file system.
     * @param cfgPath Additional path to Hadoop configuration.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(@Nullable String uri, @Nullable String cfgPath)
        throws IgniteCheckedException {
        this(uri, cfgPath, null);
    }

    /**
     * Constructor.
     *
     * @param uri URI of file system.
     * @param cfgPath Additional path to Hadoop configuration.
     * @param userName User name.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(@Nullable String uri, @Nullable String cfgPath,
        @Nullable String userName) throws IgniteCheckedException {
        setDefaultUserName(userName);

        CachingHadoopFileSystemFactory fac = new CachingHadoopFileSystemFactory();

        fac.setUri(uri);

        if (cfgPath != null)
            fac.setConfigPaths(cfgPath);

        setFileSystemFactory(fac);
    }

    /**
     * Gets default user name.
     * <p>
     * Defines user name which will be used during file system invocation in case no user name is defined explicitly
     * through {@link FileSystem#get(URI, Configuration, String)}.
     * <p>
     * Also this name will be used if you manipulate {@link IgniteFileSystem} directly and do not set user name
     * explicitly using {@link IgfsUserContext#doAs(String, IgniteOutClosure)} or
     * {@link IgfsUserContext#doAs(String, Callable)} methods.
     * <p>
     * If not set value of system property {@code "user.name"} will be used. If this property is not set either,
     * {@code "anonymous"} will be used.
     *
     * @return Default user name.
     */
    @Nullable public String getDefaultUserName() {
        return dfltUsrName;
    }

    /**
     * Sets default user name. See {@link #getDefaultUserName()} for details.
     *
     * @param dfltUsrName Default user name.
     */
    public void setDefaultUserName(@Nullable String dfltUsrName) {
        this.dfltUsrName = dfltUsrName;
    }

    /**
     * Gets secondary file system factory.
     * <p>
     * This factory will be used whenever a call to a target {@link FileSystem} is required.
     * <p>
     * If not set, {@link CachingHadoopFileSystemFactory} will be used.
     *
     * @return Secondary file system factory.
     */
    public HadoopFileSystemFactory getFileSystemFactory() {
        return fsFactory;
    }

    /**
     * Sets secondary file system factory. See {@link #getFileSystemFactory()} for details.
     *
     * @param factory Secondary file system factory.
     */
    public void setFileSystemFactory(HadoopFileSystemFactory factory) {
        this.fsFactory = factory;
    }

    /**
     * Convert IGFS path into Hadoop path.
     *
     * @param path IGFS path.
     * @return Hadoop path.
     */
    private Path convert(IgfsPath path) {
        URI uri = fileSystemForUser().getUri();

        return new Path(uri.getScheme(), uri.getAuthority(), path.toString());
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

        return F.asMap(PROP_PERMISSION, String.format("%04o", perm.toShort()), PROP_USER_NAME, status.getOwner(),
            PROP_GROUP_NAME, status.getGroup());
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        try {
            return fileSystemForUser().exists(convert(path));
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to check file existence [path=" + path + "]");
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile update(IgfsPath path, Map<String, String> props) {
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
        // Delegate to the secondary file system.
        try {
            if (!fileSystemForUser().rename(convert(src), convert(dest)))
                throw new IgfsException("Failed to rename (secondary file system returned false) " +
                    "[src=" + src + ", dest=" + dest + ']');
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to rename file [src=" + src + ", dest=" + dest + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        try {
            return fileSystemForUser().delete(convert(path), recursive);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to delete file [path=" + path + ", recursive=" + recursive + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        try {
            if (!fileSystemForUser().mkdirs(convert(path)))
                throw new IgniteException("Failed to make directories [path=" + path + "]");
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to make directories [path=" + path + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        try {
            if (!fileSystemForUser().mkdirs(convert(path), new HadoopIgfsProperties(props).permission()))
                throw new IgniteException("Failed to make directories [path=" + path + ", props=" + props + "]");
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to make directories [path=" + path + ", props=" + props + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) {
        try {
            FileStatus[] statuses = fileSystemForUser().listStatus(convert(path));

            if (statuses == null)
                throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);

            Collection<IgfsPath> res = new ArrayList<>(statuses.length);

            for (FileStatus status : statuses)
                res.add(new IgfsPath(path, status.getPath().getName()));

            return res;
        }
        catch (FileNotFoundException ignored) {
            throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to list statuses due to secondary file system exception: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) {
        try {
            FileStatus[] statuses = fileSystemForUser().listStatus(convert(path));

            if (statuses == null)
                throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);

            Collection<IgfsFile> res = new ArrayList<>(statuses.length);

            for (FileStatus status : statuses) {
                IgfsFileInfo fsInfo = status.isDirectory() ? new IgfsFileInfo(true, properties(status)) :
                    new IgfsFileInfo((int)status.getBlockSize(), status.getLen(), null, null, false,
                    properties(status));

                res.add(new IgfsFileImpl(new IgfsPath(path, status.getPath().getName()), fsInfo, 1));
            }

            return res;
        }
        catch (FileNotFoundException ignored) {
            throw new IgfsPathNotFoundException("Failed to list files (path not found): " + path);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to list statuses due to secondary file system exception: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) {
        return new HadoopIgfsSecondaryFileSystemPositionedReadable(fileSystemForUser(), convert(path), bufSize);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) {
        try {
            return fileSystemForUser().create(convert(path), overwrite);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to create file [path=" + path + ", overwrite=" + overwrite + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        HadoopIgfsProperties props0 =
            new HadoopIgfsProperties(props != null ? props : Collections.<String, String>emptyMap());

        try {
            return fileSystemForUser().create(convert(path), props0.permission(), overwrite, bufSize,
                (short) replication, blockSize, null);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to create file [path=" + path + ", props=" + props +
                ", overwrite=" + overwrite + ", bufSize=" + bufSize + ", replication=" + replication +
                ", blockSize=" + blockSize + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        try {
            return fileSystemForUser().append(convert(path), bufSize);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to append file [path=" + path + ", bufSize=" + bufSize + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(final IgfsPath path) {
        try {
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
            // We don't use FileSystem#getUsed() since it counts only the files
            // in the filesystem root, not all the files recursively.
            return fileSystemForUser().getContentSummary(new Path("/")).getSpaceConsumed();
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to get used space size of file system.");
        }
    }

    /**
     * Gets the underlying {@link FileSystem}.
     * This method is used solely for testing.
     * @return the underlying Hadoop {@link FileSystem}.
     */
    public FileSystem fileSystem() {
        return fileSystemForUser();
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
        dfltUsrName = IgfsUtils.fixUserName(dfltUsrName);

        if (fsFactory == null)
            fsFactory = new CachingHadoopFileSystemFactory();

        if (fsFactory instanceof LifecycleAware)
            ((LifecycleAware) fsFactory).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (fsFactory instanceof LifecycleAware)
             ((LifecycleAware)fsFactory).stop();
    }

    /** {@inheritDoc} */
    @Override public HadoopFileSystemFactory getPayload() {
        return fsFactory;
    }
}