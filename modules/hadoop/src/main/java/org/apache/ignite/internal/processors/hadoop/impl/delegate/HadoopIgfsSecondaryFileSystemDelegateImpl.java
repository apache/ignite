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

package org.apache.ignite.internal.processors.hadoop.impl.delegate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopIgfsSecondaryFileSystemDelegate;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsProperties;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.processors.igfs.IgfsBlockLocationImpl;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsFileImpl;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Secondary file system implementation.
 */
@SuppressWarnings("unused")
public class HadoopIgfsSecondaryFileSystemDelegateImpl implements HadoopIgfsSecondaryFileSystemDelegate {
    /** The default user name. It is used if no user context is set. */
    private final String dfltUsrName;

    /** Factory. */
    private final HadoopFileSystemFactoryDelegate factory;

    /**
     * Constructor.
     *
     * @param proxy Proxy.
    */
    public HadoopIgfsSecondaryFileSystemDelegateImpl(IgniteHadoopIgfsSecondaryFileSystem proxy) {
        assert proxy.getFileSystemFactory() != null;

        dfltUsrName = IgfsUtils.fixUserName(proxy.getDefaultUserName());

        HadoopFileSystemFactory factory0 = proxy.getFileSystemFactory();

        if (factory0 == null)
            factory0 = new CachingHadoopFileSystemFactory();

        factory = HadoopDelegateUtils.fileSystemFactoryDelegate(getClass().getClassLoader(), factory0);
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

        Path hadoopPath = convert(path);

        try {
            if (!fileSys.exists(hadoopPath))
                return null;

            if (props0.userName() != null || props0.groupName() != null)
                fileSys.setOwner(hadoopPath, props0.userName(), props0.groupName());

            if (props0.permission() != null)
                fileSys.setPermission(hadoopPath, props0.permission());
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to update file properties [path=" + path + "]");
        }

        return info(path);
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

            for (FileStatus s : statuses) {
                IgfsEntryInfo fsInfo = s.isDirectory() ?
                    IgfsUtils.createDirectory(
                        IgniteUuid.randomUuid(),
                        null,
                        properties(s),
                        s.getAccessTime(),
                        s.getModificationTime()
                    ) :
                    IgfsUtils.createFile(
                        IgniteUuid.randomUuid(),
                        (int)s.getBlockSize(),
                        s.getLen(),
                        null,
                        null,
                        false,
                        properties(s),
                        s.getAccessTime(),
                        s.getModificationTime()
                    );

                res.add(new IgfsFileImpl(new IgfsPath(path, s.getPath().getName()), fsInfo, 1));
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
        HadoopIgfsProperties props0 = new HadoopIgfsProperties(props);

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
            Path hadoopPath = convert(path);

            FileSystem fs = fileSystemForUser();

            if (create && !fs.exists(hadoopPath))
                return fs.create(hadoopPath, false, bufSize);
            else
                return fs.append(convert(path), bufSize);
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

            return new IgfsFileImpl(new IgfsFile() {
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
            }, 0);
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

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long modificationTime, long accessTime) throws IgniteException {
        try {
            // We don't use FileSystem#getUsed() since it counts only the files
            // in the filesystem root, not all the files recursively.
            fileSystemForUser().setTimes(convert(path), modificationTime, accessTime);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed set times for path: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len,
        long maxLen) throws IgniteException {
        try {
            BlockLocation[] hadoopBlocks = fileSystemForUser().getFileBlockLocations(convert(path), start, len);

            List<IgfsBlockLocation> blks = new ArrayList<>(hadoopBlocks.length);

            for (int i = 0; i < hadoopBlocks.length; ++i)
                blks.add(convertBlockLocation(hadoopBlocks[i]));

            return blks;
        }
        catch (FileNotFoundException ignored) {
            return Collections.emptyList();
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed affinity for path: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        factory.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        factory.stop();
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
     * Convert IGFS affinity block location into Hadoop affinity block location.
     *
     * @param block IGFS affinity block location.
     * @return Hadoop affinity block location.
     */
    private IgfsBlockLocation convertBlockLocation(BlockLocation block) {
        try {
            String[] names = block.getNames();
            String[] hosts = block.getHosts();

            return new IgfsBlockLocationImpl(
                block.getOffset(), block.getLength(),
                Arrays.asList(names), Arrays.asList(hosts));
        } catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed convert block location: " + block);
        }
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
            return (FileSystem)factory.get(user);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopIgfsSecondaryFileSystemDelegateImpl.class, this);
    }
}
