/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.hadoop.v1;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.ggfs.GridGgfs.*;
import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;
import static org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopParameters.*;
import static org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopUtils.*;

/**
 * {@code GGFS} Hadoop 1.x file system driver over file system API. To use
 * {@code GGFS} as Hadoop file system, you should configure this class
 * in Hadoop's {@code core-site.xml} as follows:
 * <pre name="code" class="xml">
 *  &lt;property&gt;
 *      &lt;name&gt;fs.default.name&lt;/name&gt;
 *      &lt;value&gt;ggfs://ipc&lt;/value&gt;
 *  &lt;/property&gt;
 *
 *  &lt;property&gt;
 *      &lt;name&gt;fs.ggfs.impl&lt;/name&gt;
 *      &lt;value&gt;org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopFileSystem&lt;/value&gt;
 *  &lt;/property&gt;
 * </pre>
 * You should also add GridGain JAR and all libraries to Hadoop classpath. To
 * do this, add following lines to {@code conf/hadoop-env.sh} script in Hadoop
 * distribution:
 * <pre name="code" class="bash">
 * export GRIDGAIN_HOME=/path/to/GridGain/distribution
 * export HADOOP_CLASSPATH=$GRIDGAIN_HOME/gridgain*.jar
 *
 * for f in $GRIDGAIN_HOME/libs/*.jar; do
 *  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f;
 * done
 * </pre>
 * <h1 class="header">Data vs Clients Nodes</h1>
 * Hadoop needs to use its FileSystem remotely from client nodes as well as directly on
 * data nodes. Client nodes are responsible for basic file system operations as well as
 * accessing data nodes remotely. Usually, client nodes are started together
 * with {@code job-submitter} or {@code job-scheduler} processes, while data nodes are usually
 * started together with Hadoop {@code task-tracker} processes.
 * <p>
 * For sample client and data node configuration refer to {@code config/hadoop/default-config-client.xml}
 * and {@code config/hadoop/default-config.xml} configuration files in GridGain installation.
 */
public class GridGgfsHadoopFileSystem extends FileSystem {
    /** Internal property to indicate management connection. */
    public static final String GGFS_MANAGEMENT = "fs.ggfs.management.connection";

    /** Empty array of file block locations. */
    private static final BlockLocation[] EMPTY_BLOCK_LOCATIONS = new BlockLocation[0];

    /** Empty array of file statuses. */
    public static final FileStatus[] EMPTY_FILE_STATUS = new FileStatus[0];

    /** Ensures that close routine is invoked at most once. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Grid remote client. */
    private GridGgfsHadoopWrapper rmtClient;

    /** User name for each thread. */
    private final ThreadLocal<String> userName = new ThreadLocal<String>(){
        /** {@inheritDoc} */
        @Override protected String initialValue() {
            return DFLT_USER_NAME;
        }
    };

    /** Working directory for each thread. */
    private final ThreadLocal<Path> workingDir = new ThreadLocal<Path>(){
        /** {@inheritDoc} */
        @Override protected Path initialValue() {
            return getHomeDirectory();
        }
    };

    /** Default replication factor. */
    private short dfltReplication;

    /** Base file system uri. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private URI uri;

    /** Authority. */
    private String uriAuthority;

    /** Client logger. */
    private GridGgfsHadoopLogger clientLog;

    /** Secondary URI string. */
    private URI secondaryUri;

    /** GGFS mode resolver. */
    private GridGgfsModeResolver modeRslvr;

    /** Secondary file system instance. */
    private FileSystem secondaryFs;

    /** Management connection flag. */
    private boolean mgmt;

    /** Whether custom sequential reads before prefetch value is provided. */
    private boolean seqReadsBeforePrefetchOverride;

    /** GGFS group block size. */
    private long ggfsGrpBlockSize;

    /** Flag that controls whether file wites should be colocated. */
    private boolean colocateFileWrites;

    /** Custom-provided sequential reads before prefetch. */
    private int seqReadsBeforePrefetch;

    /** The cache was disabled when the instance was creating. */
    private boolean cacheEnabled;

    /** {@inheritDoc} */
    @Override public URI getUri() {
        if (uri == null)
            throw new IllegalStateException("URI is null (was GridGgfsHadoopFileSystem properly initialized?).");

        return uri;
    }

    /**
     * Enter busy state.
     *
     * @throws IOException If file system is stopped.
     */
    private void enterBusy() throws IOException {
        if (closeGuard.get())
            throw new IOException("File system is stopped.");
    }

    /**
     * Leave busy state.
     */
    private void leaveBusy() {
        // No-op.
    }

    /**
     * Public setter that can be used by direct users of FS or Visor.
     *
     * @param colocateFileWrites Whether all ongoing file writes should be colocated.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void colocateFileWrites(boolean colocateFileWrites) {
        this.colocateFileWrites = colocateFileWrites;
    }

    /** {@inheritDoc} */
    @Override public void initialize(URI name, Configuration cfg) throws IOException {
        enterBusy();

        try {
            if (rmtClient != null)
                throw new IOException("File system is already initialized: " + rmtClient);

            A.notNull(name, "name");
            A.notNull(cfg, "cfg");

            super.initialize(name, cfg);

            setConf(cfg);

            String disableCacheName = String.format("fs.%s.impl.disable.cache", name.getScheme());

            cacheEnabled = !cfg.getBoolean(disableCacheName, false);

            mgmt = cfg.getBoolean(GGFS_MANAGEMENT, false);

            if (!GGFS_SCHEME.equals(name.getScheme()))
                throw new IOException("Illegal file system URI [expected=" + GGFS_SCHEME +
                    "://[name]/[optional_path], actual=" + name + ']');

            uri = name;

            uriAuthority = uri.getAuthority();

            setUser(cfg.get(MRJobConfig.USER_NAME, DFLT_USER_NAME));

            // Override sequential reads before prefetch if needed.
            seqReadsBeforePrefetch = parameter(cfg, PARAM_GGFS_SEQ_READS_BEFORE_PREFETCH, uriAuthority, 0);

            if (seqReadsBeforePrefetch > 0)
                seqReadsBeforePrefetchOverride = true;

            // In GG replication factor is controlled by data cache affinity.
            // We use replication factor to force the whole file to be stored on local node.
            dfltReplication = (short)cfg.getInt("dfs.replication", 3);

            // Get file colocation control flag.
            colocateFileWrites = parameter(cfg, PARAM_GGFS_COLOCATED_WRITES, uriAuthority, false);

            // Get log directory.
            String logDirCfg = parameter(cfg, PARAM_GGFS_LOG_DIR, uriAuthority, DFLT_GGFS_LOG_DIR);

            File logDirFile = U.resolveGridGainPath(logDirCfg);

            String logDir = logDirFile != null ? logDirFile.getAbsolutePath() : null;

            rmtClient = new GridGgfsHadoopWrapper(uriAuthority, logDir, cfg, LOG);

            // Handshake.
            GridGgfsHandshakeResponse handshake = rmtClient.handshake(logDir);

            ggfsGrpBlockSize = handshake.blockSize();

            GridGgfsPaths paths = handshake.secondaryPaths();

            // Initialize client logger.
            Boolean logEnabled = parameter(cfg, PARAM_GGFS_LOG_ENABLED, uriAuthority, false);

            if (handshake.sampling() != null ? handshake.sampling() : logEnabled) {
                // Initiate client logger.
                if (logDir == null)
                    throw new IOException("Failed to resolve log directory: " + logDirCfg);

                Integer batchSize = parameter(cfg, PARAM_GGFS_LOG_BATCH_SIZE, uriAuthority, DFLT_GGFS_LOG_BATCH_SIZE);

                clientLog = GridGgfsHadoopLogger.logger(uriAuthority, handshake.ggfsName(), logDir, batchSize);
            }
            else
                clientLog = GridGgfsHadoopLogger.disabledLogger();

            modeRslvr = new GridGgfsModeResolver(paths.defaultMode(), paths.pathModes());

            boolean initSecondary = paths.defaultMode() == PROXY;

            if (paths.pathModes() != null && !paths.pathModes().isEmpty()) {
                for (T2<GridGgfsPath, GridGgfsMode> pathMode : paths.pathModes()) {
                    GridGgfsMode mode = pathMode.getValue();

                    initSecondary |= mode == PROXY;
                }
            }

            if (initSecondary) {
                if (paths.secondaryConfigurationPath() == null)
                    throw new IOException("Failed to connect to the secondary file system because configuration " +
                        "path is not provided.");

                if (paths.secondaryUri() == null)
                    throw new IOException("Failed to connect to the secondary file system because URI is not " +
                        "provided.");

                String secondaryConfPath = paths.secondaryConfigurationPath();

                try {
                    secondaryUri = new URI(paths.secondaryUri());

                    URL secondaryCfgUrl = U.resolveGridGainUrl(secondaryConfPath);

                    Configuration conf = new Configuration();

                    if (secondaryCfgUrl != null)
                        conf.addResource(secondaryCfgUrl);

                    String prop = String.format("fs.%s.impl.disable.cache", secondaryUri.getScheme());

                    conf.setBoolean(prop, true);

                    secondaryFs = FileSystem.get(secondaryUri, conf);
                }
                catch (URISyntaxException ignore) {
                    if (!mgmt)
                        throw new IOException("Failed to resolve secondary file system URI: " + paths.secondaryUri());
                    else
                        LOG.warn("Visor failed to create secondary file system (operations on paths with PROXY mode " +
                            "will have no effect).");
                }
                catch (IOException e) {
                    if (!mgmt)
                        throw new IOException("Failed to connect to the secondary file system: " +
                            paths.secondaryUri(), e);
                    else
                        LOG.warn("Visor failed to create secondary file system (operations on paths with PROXY mode " +
                            "will have no effect): " + e.getMessage());
                }
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkPath(Path path) {
        URI uri = path.toUri();

        if (uri.isAbsolute()) {
            if (!F.eq(uri.getScheme(), GGFS_SCHEME))
                throw new InvalidPathException("Wrong path scheme [expected=" + GGFS_SCHEME + ", actual=" +
                    uri.getAuthority() + ']');

            if (!F.eq(uri.getAuthority(), uriAuthority))
                throw new InvalidPathException("Wrong path authority [expected=" + uriAuthority + ", actual=" +
                    uri.getAuthority() + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public short getDefaultReplication() {
        return dfltReplication;
    }

    /** {@inheritDoc} */
    @Override protected void finalize() throws Throwable {
        super.finalize();

        close0();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (cacheEnabled && get(getUri(), getConf()) == this)
            return;

        close0();
    }

    /**
     * Closes file system.
     *
     * @throws IOException If failed.
     */
    private void close0() throws IOException {
        if (closeGuard.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled())
                LOG.debug("File system closed [uri=" + uri + ", endpoint=" + uriAuthority + ']');

            if (rmtClient == null)
                return;

            super.close();

            rmtClient.close(false);

            if (clientLog.isLogEnabled())
                clientLog.close();

            if (secondaryFs != null)
                U.closeQuiet(secondaryFs);

            // Reset initialized resources.
            uri = null;
            rmtClient = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void setTimes(Path p, long mtime, long atime) throws IOException {
        enterBusy();

        try {
            A.notNull(p, "p");

            if (mode(p) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    // No-op for management connection.
                    return;
                }

                secondaryFs.setTimes(toSecondary(p), mtime, atime);
            }
            else {
                GridGgfsPath path = convert(p);

                rmtClient.setTimes(path, atime, mtime);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void setPermission(Path p, FsPermission perm) throws IOException {
        enterBusy();

        try {
            A.notNull(p, "p");

            if (mode(p) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    // No-op for management connection.
                    return;
                }

                secondaryFs.setPermission(toSecondary(p), perm);
            }
            else if (rmtClient.update(convert(p), permission(perm)) == null) {
                    throw new IOException("Failed to set file permission (file not found?)" +
                        " [path=" + p + ", perm=" + perm + ']');
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void setOwner(Path p, String username, String grpName) throws IOException {
        A.notNull(p, "p");
        A.notNull(username, "username");
        A.notNull(grpName, "grpName");

        enterBusy();

        try {
            if (mode(p) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    // No-op for management connection.
                    return;
                }

                secondaryFs.setOwner(toSecondary(p), username, grpName);
            }
            else if (rmtClient.update(convert(p), F.asMap(PROP_USER_NAME, username, PROP_GROUP_NAME, grpName)) == null)
                throw new IOException("Failed to set file permission (file not found?)" +
                    " [path=" + p + ", userName=" + username + ", groupName=" + grpName + ']');
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FSDataInputStream open(Path f, int bufSize) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to open file (secondary file system is not initialized): " + f);
                }

                FSDataInputStream is = secondaryFs.open(toSecondary(f), bufSize);

                if (clientLog.isLogEnabled()) {
                    // At this point we do not know file size, so we perform additional request to remote FS to get it.
                    FileStatus status = secondaryFs.getFileStatus(toSecondary(f));

                    long size = status != null ? status.getLen() : -1;

                    long logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logOpen(logId, path, PROXY, bufSize, size);

                    return new FSDataInputStream(new GridGgfsHadoopProxyInputStream(is, clientLog, logId));
                }
                else
                    return is;
            }
            else {
                GridGgfsHadoopStreamDelegate stream = seqReadsBeforePrefetchOverride ?
                    rmtClient.open(path, seqReadsBeforePrefetch) : rmtClient.open(path);

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logOpen(logId, path, mode, bufSize, stream.length());
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opening input stream [thread=" + Thread.currentThread().getName() + ", path=" + path +
                        ", bufSize=" + bufSize + ']');

                GridGgfsHadoopInputStream ggfsIn = new GridGgfsHadoopInputStream(stream, stream.length(),
                    bufSize, LOG, clientLog, logId);

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened input stream [path=" + path + ", delegate=" + stream + ']');

                return new FSDataInputStream(ggfsIn);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public FSDataOutputStream create(Path f, FsPermission perm, boolean overwrite, int bufSize,
        short replication, long blockSize, Progressable progress) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        OutputStream out = null;

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (LOG.isDebugEnabled())
                LOG.debug("Opening output stream in create [thread=" + Thread.currentThread().getName() + "path=" +
                    path + ", overwrite=" + overwrite + ", bufSize=" + bufSize + ']');

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to create file (secondary file system is not initialized): " + f);
                }

                FSDataOutputStream os =
                    secondaryFs.create(toSecondary(f), perm, overwrite, bufSize, replication, blockSize, progress);

                if (clientLog.isLogEnabled()) {
                    long logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logCreate(logId, path, PROXY, overwrite, bufSize, replication, blockSize);

                    return new FSDataOutputStream(new GridGgfsHadoopProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                // Create stream and close it in the 'finally' section if any sequential operation failed.
                GridGgfsHadoopStreamDelegate stream = rmtClient.create(path, overwrite, colocateFileWrites,
                    replication, blockSize, permission(perm));

                assert stream != null;

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logCreate(logId, path, mode, overwrite, bufSize, replication, blockSize);
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened output stream in create [path=" + path + ", delegate=" + stream + ']');

                GridGgfsHadoopOutputStream ggfsOut = new GridGgfsHadoopOutputStream(stream, LOG, clientLog,
                    logId);

                bufSize = Math.max(64 * 1024, bufSize);

                out = new BufferedOutputStream(ggfsOut, bufSize);

                FSDataOutputStream res = new FSDataOutputStream(out, null, 0);

                // Mark stream created successfully.
                out = null;

                return res;
            }
        }
        finally {
            // Close if failed during stream creation.
            if (out != null)
                U.closeQuiet(out);

            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public FSDataOutputStream append(Path f, int bufSize, Progressable progress) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (LOG.isDebugEnabled())
                LOG.debug("Opening output stream in append [thread=" + Thread.currentThread().getName() +
                    ", path=" + path + ", bufSize=" + bufSize + ']');

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to append file (secondary file system is not initialized): " + f);
                }

                FSDataOutputStream os = secondaryFs.append(toSecondary(f), bufSize, progress);

                if (clientLog.isLogEnabled()) {
                    long logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logAppend(logId, path, PROXY, bufSize); // Don't have stream ID.

                    return new FSDataOutputStream(new GridGgfsHadoopProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                GridGgfsHadoopStreamDelegate stream = rmtClient.append(path, false, null);

                assert stream != null;

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = GridGgfsHadoopLogger.nextId();

                    clientLog.logAppend(logId, path, mode, bufSize);
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened output stream in append [path=" + path + ", delegate=" + stream + ']');

                GridGgfsHadoopOutputStream ggfsOut = new GridGgfsHadoopOutputStream(stream, LOG, clientLog,
                    logId);

                bufSize = Math.max(64 * 1024, bufSize);

                BufferedOutputStream out = new BufferedOutputStream(ggfsOut, bufSize);

                return new FSDataOutputStream(out, null, 0);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean rename(Path src, Path dst) throws IOException {
        A.notNull(src, "src");
        A.notNull(dst, "dst");

        enterBusy();

        try {
            GridGgfsPath srcPath = convert(src);
            GridGgfsPath dstPath = convert(dst);
            GridGgfsMode mode = mode(srcPath);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return false;
                }

                if (clientLog.isLogEnabled())
                    clientLog.logRename(srcPath, PROXY, dstPath);

                return secondaryFs.rename(toSecondary(src), toSecondary(dst));
            }
            else {
                // Will throw exception if failed.
                rmtClient.rename(srcPath, dstPath);

                if (clientLog.isLogEnabled())
                    clientLog.logRename(srcPath, mode, dstPath);

                return true;
            }
        }
        catch (IOException e) {
            // Intentionally ignore GGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !GridGgfsException.class.isAssignableFrom(e.getCause().getClass())))
                throw e;
            else
                return false;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean delete(Path f) throws IOException {
        return delete(f, false);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return false;
                }

                if (clientLog.isLogEnabled())
                    clientLog.logDelete(path, PROXY, recursive);

                return secondaryFs.delete(toSecondary(f), recursive);
            }
            else {
                // Will throw exception if delete failed.
                boolean res = rmtClient.delete(path, recursive);

                if (clientLog.isLogEnabled())
                    clientLog.logDelete(path, mode, recursive);

                return res;
            }
        }
        catch (IOException e) {
            // Intentionally ignore GGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !GridGgfsException.class.isAssignableFrom(e.getCause().getClass())))
                throw e;
            else
                return false;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FileStatus[] listStatus(Path f) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return EMPTY_FILE_STATUS;
                }

                FileStatus[] arr = secondaryFs.listStatus(toSecondary(f));

                if (arr != null) {
                    for (int i = 0; i < arr.length; i++)
                        arr[i] = toPrimary(arr[i]);
                }

                if (clientLog.isLogEnabled()) {
                    String[] fileArr = null;

                    if (arr != null) {
                        fileArr = new String[arr.length];

                        for (int i = 0; i < arr.length; i++)
                            fileArr[i] = arr[i].getPath().toString();
                    }

                    clientLog.logListDirectory(path, PROXY, fileArr);
                }

                return arr;
            }
            else {
                Collection<GridGgfsFile> list = rmtClient.listFiles(path);

                if (list == null)
                    return null;

                List<GridGgfsFile> files = new ArrayList<>(list);

                FileStatus[] arr = new FileStatus[files.size()];

                for (int i = 0; i < arr.length; i++)
                    arr[i] = convert(files.get(i));

                if (clientLog.isLogEnabled()) {
                    String[] fileArr = new String[arr.length];

                    for (int i = 0; i < arr.length; i++)
                        fileArr[i] = arr[i].getPath().toString();

                    clientLog.logListDirectory(path, mode, fileArr);
                }

                return arr;
            }
        }
        catch (FileNotFoundException ignored) {
            return null;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Path getHomeDirectory() {
        Path path = new Path("/user/" + userName.get());

        return path.makeQualified(getUri(), null);
    }

    /**
     * Set user name and default working directory for current thread.
     *
     * @param userName User name.
     */
    public void setUser(String userName) {
        this.userName.set(userName);

        setWorkingDirectory(null);
    }

    /** {@inheritDoc} */
    @Override public void setWorkingDirectory(Path newPath) {
        if (newPath == null) {
            Path homeDir = getHomeDirectory();

            if (secondaryFs != null)
                secondaryFs.setWorkingDirectory(toSecondary(homeDir));

            workingDir.set(homeDir);
        }
        else {
            Path fixedNewPath = fixRelativePart(newPath);

            String res = fixedNewPath.toUri().getPath();

            if (!DFSUtil.isValidName(res))
                throw new IllegalArgumentException("Invalid DFS directory name " + res);

            if (secondaryFs != null)
                secondaryFs.setWorkingDirectory(toSecondary(fixedNewPath));

            workingDir.set(fixedNewPath);
        }
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workingDir.get();
    }

    /** {@inheritDoc} */
    @Override public boolean mkdirs(Path f, FsPermission perm) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            GridGgfsPath path = convert(f);
            GridGgfsMode mode = mode(path);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return false;
                }

                if (clientLog.isLogEnabled())
                    clientLog.logMakeDirectory(path, PROXY);

                return secondaryFs.mkdirs(toSecondary(f), perm);
            }
            else {
                boolean mkdirRes = rmtClient.mkdirs(path, permission(perm));

                if (clientLog.isLogEnabled())
                    clientLog.logMakeDirectory(path, mode);

                return mkdirRes;
            }
        }
        catch (IOException e) {
            // Intentionally ignore GGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !GridGgfsException.class.isAssignableFrom(e.getCause().getClass())))
                throw e;
            else
                return false;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FileStatus getFileStatus(Path f) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            if (mode(f) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to get file status (secondary file system is not initialized): " + f);
                }

                return toPrimary(secondaryFs.getFileStatus(toSecondary(f)));
            }
            else {
                GridGgfsFile info = rmtClient.info(convert(f));

                if (info == null)
                    throw new FileNotFoundException("File not found: " + f);

                return convert(info);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public ContentSummary getContentSummary(Path f) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            if (mode(f) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to get content summary (secondary file system is not initialized): " +
                        f);
                }

                return secondaryFs.getContentSummary(toSecondary(f));
            }
            else {
                GridGgfsPathSummary sum = rmtClient.contentSummary(convert(f));

                return new ContentSummary(sum.totalLength(), sum.filesCount(), sum.directoriesCount(),
                    -1, sum.totalLength(), rmtClient.fsStatus().spaceTotal());
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long len) throws IOException {
        A.notNull(status, "status");

        enterBusy();

        try {
            GridGgfsPath path = convert(status.getPath());

            if (mode(status.getPath()) == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return EMPTY_BLOCK_LOCATIONS;
                }

                Path secPath = toSecondary(status.getPath());

                return secondaryFs.getFileBlockLocations(secondaryFs.getFileStatus(secPath), start, len);
            }
            else {
                long now = System.currentTimeMillis();

                List<GridGgfsBlockLocation> affinity = new ArrayList<>(rmtClient.affinity(path, start, len));

                BlockLocation[] arr = new BlockLocation[affinity.size()];

                for (int i = 0; i < arr.length; i++)
                    arr[i] = convert(affinity.get(i));

                if (LOG.isDebugEnabled())
                    LOG.debug("Fetched file locations [path=" + path + ", fetchTime=" +
                        (System.currentTimeMillis() - now) + ", locations=" + Arrays.asList(arr) + ']');

                return arr;
            }
        }
        catch (FileNotFoundException ignored) {
            return EMPTY_BLOCK_LOCATIONS;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public long getDefaultBlockSize() {
        return ggfsGrpBlockSize;
    }

    /**
     * Resolve path mode.
     *
     * @param path HDFS path.
     * @return Path mode.
     */
    public GridGgfsMode mode(Path path) {
        return mode(convert(path));
    }

    /**
     * Resolve path mode.
     *
     * @param path GGFS path.
     * @return Path mode.
     */
    public GridGgfsMode mode(GridGgfsPath path) {
        return modeRslvr.resolveMode(path);
    }

    /**
     * Convert the given path to path acceptable by the primary file system.
     *
     * @param path Path.
     * @return Primary file system path.
     */
    private Path toPrimary(Path path) {
        return convertPath(path, uri);
    }

    /**
     * Convert the given path to path acceptable by the secondary file system.
     *
     * @param path Path.
     * @return Secondary file system path.
     */
    private Path toSecondary(Path path) {
        assert secondaryFs != null;
        assert secondaryUri != null;

        return convertPath(path, secondaryUri);
    }

    /**
     * Convert path using the given new URI.
     *
     * @param path Old path.
     * @param newUri New URI.
     * @return New path.
     */
    private Path convertPath(Path path, URI newUri) {
        assert newUri != null;

        if (path != null) {
            URI pathUri = path.toUri();

            try {
                return new Path(new URI(pathUri.getScheme() != null ? newUri.getScheme() : null,
                    pathUri.getAuthority() != null ? newUri.getAuthority() : null, pathUri.getPath(), null, null));
            }
            catch (URISyntaxException e) {
                throw new GridRuntimeException("Failed to construct secondary file system path from the primary file " +
                    "system path: " + path, e);
            }
        }
        else
            return null;
    }

    /**
     * Convert a file status obtained from the secondary file system to a status of the primary file system.
     *
     * @param status Secondary file system status.
     * @return Primary file system status.
     */
    @SuppressWarnings("deprecation")
    private FileStatus toPrimary(FileStatus status) {
        return status != null ? new FileStatus(status.getLen(), status.isDir(), status.getReplication(),
            status.getBlockSize(), status.getModificationTime(), status.getAccessTime(), status.getPermission(),
            status.getOwner(), status.getGroup(), toPrimary(status.getPath())) : null;
    }

    /**
     * Convert GGFS path into Hadoop path.
     *
     * @param path GGFS path.
     * @return Hadoop path.
     */
    private Path convert(GridGgfsPath path) {
        return new Path(GGFS_SCHEME, uriAuthority, path.toString());
    }

    /**
     * Convert Hadoop path into GGFS path.
     *
     * @param path Hadoop path.
     * @return GGFS path.
     */
    @Nullable private GridGgfsPath convert(@Nullable Path path) {
        if (path == null)
            return null;

        return path.isAbsolute() ? new GridGgfsPath(path.toUri().getPath()) :
            new GridGgfsPath(convert(workingDir.get()), path.toUri().getPath());
    }

    /**
     * Convert GGFS affinity block location into Hadoop affinity block location.
     *
     * @param block GGFS affinity block location.
     * @return Hadoop affinity block location.
     */
    private BlockLocation convert(GridGgfsBlockLocation block) {
        Collection<String> names = block.names();
        Collection<String> hosts = block.hosts();

        return new BlockLocation(
            names.toArray(new String[names.size()]) /* hostname:portNumber of data nodes */,
            hosts.toArray(new String[hosts.size()]) /* hostnames of data nodes */,
            block.start(), block.length()
        ) {
            @Override public String toString() {
                try {
                    return "BlockLocation [offset=" + getOffset() + ", length=" + getLength() +
                        ", hosts=" + Arrays.asList(getHosts()) + ", names=" + Arrays.asList(getNames()) + ']';
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Convert GGFS file information into Hadoop file status.
     *
     * @param file GGFS file information.
     * @return Hadoop file status.
     */
    @SuppressWarnings("deprecation")
    private FileStatus convert(GridGgfsFile file) {
        return new FileStatus(file.length(), file.isDirectory(), getDefaultReplication(),
            file.groupBlockSize(), file.modificationTime(), file.accessTime(), permission(file),
            file.property(PROP_USER_NAME, DFLT_USER_NAME), file.property(PROP_GROUP_NAME, "users"),
            convert(file.path())) {
            @Override public String toString() {
                return "FileStatus [path=" + getPath() + ", isDir=" + isDir() + ", len=" + getLen() +
                    ", mtime=" + getModificationTime() + ", atime=" + getAccessTime() + ']';
            }
        };
    }

    /**
     * Convert Hadoop permission into GGFS file attribute.
     *
     * @param perm Hadoop permission.
     * @return GGFS attributes.
     */
    private Map<String, String> permission(FsPermission perm) {
        if (perm == null)
            perm = FsPermission.getDefault();

        return F.asMap(PROP_PERMISSION, String.format("%04o", perm.toShort()));
    }

    /**
     * Convert GGFS file attributes into Hadoop permission.
     *
     * @param file File info.
     * @return Hadoop permission.
     */
    private FsPermission permission(GridGgfsFile file) {
        String perm = file.property(PROP_PERMISSION, null);

        if (perm == null)
            return FsPermission.getDefault();

        try {
            return new FsPermission((short)Integer.parseInt(perm, 8));
        }
        catch (NumberFormatException ignore) {
            return FsPermission.getDefault();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsHadoopFileSystem.class, this);
    }
}
