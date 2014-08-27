package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 *
 */
public class GridGgfsHadoopFileSystemWrapper implements GridGgfsFileSystem {
    /** */
    private FileSystem fileSys;

    /** */
    private GridLogger log;

    /**
     *
     * @param fileSystem
     */
    public GridGgfsHadoopFileSystemWrapper(FileSystem fileSystem) {
        fileSys = fileSystem;
    }

    /**
     * Convert GGFS path into Hadoop path.
     *
     * @param path GGFS path.
     * @return Hadoop path.
     */
    private Path convert(GridGgfsPath path) {
        URI uri = fileSys.getUri();

        return new Path(uri.getScheme(), uri.getAuthority(), path.toString());
    }

    /**
     * Heuristically checks if exception was caused by invalid HDFS version and returns appropriate exception.
     *
     * @param e Exception to check.
     * @param detailMsg Detailed error message.
     * @return Appropriate exception.
     */
    private GridGgfsException handleSecondaryFsError(IOException e, String detailMsg) {
        boolean wrongVer = X.hasCause(e, RemoteException.class) ||
                (e.getMessage() != null && e.getMessage().contains("Failed on local"));

        GridGgfsException ggfsErr = !wrongVer ? new GridGgfsException(detailMsg, e) :
                new GridGgfsInvalidHdfsVersionException("HDFS version you are connecting to differs from local " +
                        "version (start GGFS node with '-h1' option if using HDFS ver. 1.x)", e);

        LT.error(log, ggfsErr, "Failed to connect to secondary Hadoop file system.");

        return ggfsErr;
    }

    /**
     * Convert Hadoop FileStatus properties to map.
     *
     * @param status File status.
     * @return GGFS attributes.
     */
    private static Map<String, String> properties(FileStatus status) {
        FsPermission perm = status.getPermission();

        if (perm == null)
            perm = FsPermission.getDefault();

        return F.asMap(PROP_PERMISSION, String.format("%04o", perm.toShort()), PROP_USER_NAME, status.getOwner(),
            PROP_GROUP_NAME, status.getGroup());
    }

    /** {@inheritDoc} */
    @Override public boolean exists(GridGgfsPath path) throws GridException {
        try {
            return fileSys.exists(convert(path));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException {
        GridGgfsHdfsProperties props0 = new GridGgfsHdfsProperties(props);

        try {
            if (props0.userName() != null || props0.groupName() != null)
                fileSys.setOwner(convert(path), props0.userName(), props0.groupName());

            if (props0.permission() != null)
                fileSys.setPermission(convert(path), props0.permission());
        }
        catch (IOException e) {
            throw new GridException(e);
        }

        //Is not used in secondary FS.
        return null;
    }

    /** {@inheritDoc} */
    @Override public void rename(GridGgfsPath src, GridGgfsPath dest) throws GridException {
        // Delegate to the secondary file system.
        try {
            if (!fileSys.rename(convert(src), convert(dest)))
                throw new GridGgfsException("Failed to rename (secondary file system returned false) " +
                    "[src=" + src + ", dest=" + dest + ']');
        }
        catch (IOException e) {
            throw new GridException(e);
        }

    }

    /** {@inheritDoc} */
    @Override public boolean delete(GridGgfsPath path, boolean recursive) throws GridException {
        try {
            return fileSys.delete(convert(path), recursive);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(GridGgfsPath path) throws GridException {
        try {
            if (!fileSys.mkdirs(convert(path)))
                throw new GridException("Failed to make directories [path=" + path + "]");
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(GridGgfsPath path, @Nullable Map<String, String> props) throws GridException {
        try {
            if (!fileSys.mkdirs(convert(path), new GridGgfsHdfsProperties(props).permission()))
                throw new GridException("Failed to make directories [path=" + path + ", props=" + props + "]");
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException {
        Collection<GridGgfsPath> res = new ArrayList<>();

        try {
            FileStatus[] statuses = fileSys.listStatus(convert(path));

            if (statuses == null)
                throw new GridGgfsFileNotFoundException("Failed to list files (path not found): " + path);

            for (FileStatus status : statuses)
                res.add(new GridGgfsPath(path, status.getPath().getName()));
        }
        catch (FileNotFoundException ignored) {
            throw new GridGgfsFileNotFoundException("Failed to list files (path not found): " + path);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to list statuses due to secondary file system exception: " + path);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException {
        Collection<GridGgfsFile> res = new ArrayList<>();

        try {
            FileStatus[] statuses = fileSys.listStatus(convert(path));

            if (statuses == null)
                throw new GridGgfsFileNotFoundException("Failed to list files (path not found): " + path);

            for (FileStatus status : statuses) {
                GridGgfsFileInfo fsInfo = status.isDirectory() ? new GridGgfsFileInfo(true, properties(status)) :
                    new GridGgfsFileInfo((int)status.getBlockSize(), status.getLen(), null, null, false, properties(status));

                res.add(new GridGgfsFileImpl(new GridGgfsPath(path, status.getPath().getName()), fsInfo, 1));
            }
        }
        catch (FileNotFoundException ignored) {
            throw new GridGgfsFileNotFoundException("Failed to list files (path not found): " + path);
        }
        catch (IOException e) {
            throw handleSecondaryFsError(e, "Failed to list statuses due to secondary file system exception: " + path);
        }

        return res;
    }

    @Override public GridGgfsReader openFile(GridGgfsPath path, int bufSize) {
        return new GridGgfsHadoopReader(fileSys, convert(path), bufSize, log);
    }

    @Override public GridGgfsWriter createFile(GridGgfsPath path, boolean overwrite) throws GridException {
        try {
            return new GridGgfsOutputStreamWriter(fileSys.create(convert(path), overwrite));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public GridGgfsWriter createFile(GridGgfsPath path, Map<String, String> props, boolean overwrite,
        int bufSize, short replication, long blockSize) throws GridException {
        GridGgfsHdfsProperties props0 = new GridGgfsHdfsProperties(props != null ? props :
            Collections.<String, String>emptyMap());

        try {
            return new GridGgfsOutputStreamWriter(fileSys.create(convert(path), props0.permission(), overwrite,
                bufSize, replication, blockSize, null));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public GridGgfsWriter appendFile(GridGgfsPath path, int bufSize) throws GridException {
        try {
            return new GridGgfsOutputStreamWriter(fileSys.append(convert(path), bufSize));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public GridGgfsFileStatus getFileStatus(GridGgfsPath path) throws GridException {
        try {
            final FileStatus status = fileSys.getFileStatus(convert(path));

            if (status == null)
                return null;

            return new GridGgfsFileStatus() {
                @Override public boolean isDir() {
                    return status.isDirectory();
                }

                @Override public int getBlockSize() {
                    return (int)status.getBlockSize();
                }

                @Override public long getLen() {
                    return status.getLen();
                }

                @Override public Map<String, String> properties() {
                    return GridGgfsHadoopFileSystemWrapper.properties(status);
                }
            };

        }
        catch (FileNotFoundException ignore) {
            return null;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public long usedSpaceSize() throws GridException {
        try {
            return fileSys.getContentSummary(new Path(fileSys.getUri())).getSpaceConsumed();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public void close() throws IOException {
        fileSys.close();
    }
}
