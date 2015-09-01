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

package org.apache.ignite.hadoop.fs.v1;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.processors.hadoop.SecondaryFileSystemProvider;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsInputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsOutputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProxyInputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProxyOutputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsStreamDelegate;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsWrapper;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsModeResolver;
import org.apache.ignite.internal.processors.igfs.IgfsPaths;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_BATCH_SIZE;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_DIR;
import static org.apache.ignite.igfs.IgfsMode.PROXY;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_COLOCATED_WRITES;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_LOG_BATCH_SIZE;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_LOG_DIR;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_LOG_ENABLED;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_PREFER_LOCAL_WRITES;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.parameter;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.IGFS_SCHEME;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_GROUP_NAME;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_PERMISSION;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_PREFER_LOCAL_WRITES;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_USER_NAME;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.SECONDARY_FS_CONFIG_PATH;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.SECONDARY_FS_URI;

/**
 * {@code IGFS} Hadoop 1.x file system driver over file system API. To use
 * {@code IGFS} as Hadoop file system, you should configure this class
 * in Hadoop's {@code core-site.xml} as follows:
 * <pre name="code" class="xml">
 *  &lt;property&gt;
 *      &lt;name&gt;fs.default.name&lt;/name&gt;
 *      &lt;value&gt;igfs:///&lt;/value&gt;
 *  &lt;/property&gt;
 *
 *  &lt;property&gt;
 *      &lt;name&gt;fs.igfs.impl&lt;/name&gt;
 *      &lt;value&gt;org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem&lt;/value&gt;
 *  &lt;/property&gt;
 * </pre>
 * You should also add Ignite JAR and all libraries to Hadoop classpath. To
 * do this, add following lines to {@code conf/hadoop-env.sh} script in Hadoop
 * distribution:
 * <pre name="code" class="bash">
 * export IGNITE_HOME=/path/to/Ignite/distribution
 * export HADOOP_CLASSPATH=$IGNITE_HOME/ignite*.jar
 *
 * for f in $IGNITE_HOME/libs/*.jar; do
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
 * and {@code config/hadoop/default-config.xml} configuration files in Ignite installation.
 */
public class IgniteHadoopFileSystem extends FileSystem {
    /** Internal property to indicate management connection. */
    public static final String IGFS_MANAGEMENT = "fs.igfs.management.connection";

    /** Empty array of file block locations. */
    private static final BlockLocation[] EMPTY_BLOCK_LOCATIONS = new BlockLocation[0];

    /** Empty array of file statuses. */
    public static final FileStatus[] EMPTY_FILE_STATUS = new FileStatus[0];

    /** Ensures that close routine is invoked at most once. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Grid remote client. */
    private HadoopIgfsWrapper rmtClient;

    /** working directory. */
    private Path workingDir;

    /** Default replication factor. */
    private short dfltReplication;

    /** Base file system uri. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private URI uri;

    /** Authority. */
    private String uriAuthority;

    /** Client logger. */
    private IgfsLogger clientLog;

    /** Secondary URI string. */
    private URI secondaryUri;

    /** The user name this file system was created on behalf of. */
    private String user;

    /** IGFS mode resolver. */
    private IgfsModeResolver modeRslvr;

    /** Secondary file system instance. */
    private FileSystem secondaryFs;

    /** Management connection flag. */
    private boolean mgmt;

    /** Whether custom sequential reads before prefetch value is provided. */
    private boolean seqReadsBeforePrefetchOverride;

    /** IGFS group block size. */
    private long igfsGrpBlockSize;

    /** Flag that controls whether file writes should be colocated. */
    private boolean colocateFileWrites;

    /** Prefer local writes. */
    private boolean preferLocFileWrites;

    /** Custom-provided sequential reads before prefetch. */
    private int seqReadsBeforePrefetch;

    /** {@inheritDoc} */
    @Override public URI getUri() {
        if (uri == null)
            throw new IllegalStateException("URI is null (was IgniteHadoopFileSystem properly initialized?).");

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
     * Gets non-null user name as per the Hadoop file system viewpoint.
     * @return the user name, never null.
     */
    public static String getFsHadoopUser() throws IOException {
        UserGroupInformation currUgi = UserGroupInformation.getCurrentUser();

        String user = currUgi.getShortUserName();

        user = IgfsUtils.fixUserName(user);

        assert user != null;

        return user;
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
    @SuppressWarnings("ConstantConditions")
    @Override public void initialize(URI name, Configuration cfg) throws IOException {
        enterBusy();

        try {
            if (rmtClient != null)
                throw new IOException("File system is already initialized: " + rmtClient);

            A.notNull(name, "name");
            A.notNull(cfg, "cfg");

            super.initialize(name, cfg);

            setConf(cfg);

            mgmt = cfg.getBoolean(IGFS_MANAGEMENT, false);

            if (!IGFS_SCHEME.equals(name.getScheme()))
                throw new IOException("Illegal file system URI [expected=" + IGFS_SCHEME +
                    "://[name]/[optional_path], actual=" + name + ']');

            uri = name;

            uriAuthority = uri.getAuthority();

            user = getFsHadoopUser();

            // Override sequential reads before prefetch if needed.
            seqReadsBeforePrefetch = parameter(cfg, PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH, uriAuthority, 0);

            if (seqReadsBeforePrefetch > 0)
                seqReadsBeforePrefetchOverride = true;

            // In Ignite replication factor is controlled by data cache affinity.
            // We use replication factor to force the whole file to be stored on local node.
            dfltReplication = (short)cfg.getInt("dfs.replication", 3);

            // Get file colocation control flag.
            colocateFileWrites = parameter(cfg, PARAM_IGFS_COLOCATED_WRITES, uriAuthority, false);
            preferLocFileWrites = cfg.getBoolean(PARAM_IGFS_PREFER_LOCAL_WRITES, false);

            // Get log directory.
            String logDirCfg = parameter(cfg, PARAM_IGFS_LOG_DIR, uriAuthority, DFLT_IGFS_LOG_DIR);

            File logDirFile = U.resolveIgnitePath(logDirCfg);

            String logDir = logDirFile != null ? logDirFile.getAbsolutePath() : null;

            rmtClient = new HadoopIgfsWrapper(uriAuthority, logDir, cfg, LOG, user);

            // Handshake.
            IgfsHandshakeResponse handshake = rmtClient.handshake(logDir);

            igfsGrpBlockSize = handshake.blockSize();

            IgfsPaths paths = handshake.secondaryPaths();

            // Initialize client logger.
            Boolean logEnabled = parameter(cfg, PARAM_IGFS_LOG_ENABLED, uriAuthority, false);

            if (handshake.sampling() != null ? handshake.sampling() : logEnabled) {
                // Initiate client logger.
                if (logDir == null)
                    throw new IOException("Failed to resolve log directory: " + logDirCfg);

                Integer batchSize = parameter(cfg, PARAM_IGFS_LOG_BATCH_SIZE, uriAuthority, DFLT_IGFS_LOG_BATCH_SIZE);

                clientLog = IgfsLogger.logger(uriAuthority, handshake.igfsName(), logDir, batchSize);
            }
            else
                clientLog = IgfsLogger.disabledLogger();

            modeRslvr = new IgfsModeResolver(paths.defaultMode(), paths.pathModes());

            boolean initSecondary = paths.defaultMode() == PROXY;

            if (!initSecondary && paths.pathModes() != null && !paths.pathModes().isEmpty()) {
                for (T2<IgfsPath, IgfsMode> pathMode : paths.pathModes()) {
                    IgfsMode mode = pathMode.getValue();

                    if (mode == PROXY) {
                        initSecondary = true;

                        break;
                    }
                }
            }

            if (initSecondary) {
                Map<String, String> props = paths.properties();

                String secUri = props.get(SECONDARY_FS_URI);
                String secConfPath = props.get(SECONDARY_FS_CONFIG_PATH);

                try {
                    SecondaryFileSystemProvider secProvider = new SecondaryFileSystemProvider(secUri, secConfPath);

                    secondaryFs = secProvider.createFileSystem(user);

                    secondaryUri = secProvider.uri();
                }
                catch (IOException e) {
                    if (!mgmt)
                        throw new IOException("Failed to connect to the secondary file system: " + secUri, e);
                    else
                        LOG.warn("Visor failed to create secondary file system (operations on paths with PROXY mode " +
                            "will have no effect): " + e.getMessage());
                }
            }

            // set working directory to the home directory of the current Fs user:
            setWorkingDirectory(null);
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkPath(Path path) {
        URI uri = path.toUri();

        if (uri.isAbsolute()) {
            if (!F.eq(uri.getScheme(), IGFS_SCHEME))
                throw new InvalidPathException("Wrong path scheme [expected=" + IGFS_SCHEME + ", actual=" +
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

        close();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (closeGuard.compareAndSet(false, true))
            close0();
    }

    /**
     * Closes file system.
     *
     * @throws IOException If failed.
     */
    private void close0() throws IOException {
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
                IgfsPath path = convert(p);

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
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

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

                    long logId = IgfsLogger.nextId();

                    clientLog.logOpen(logId, path, PROXY, bufSize, size);

                    return new FSDataInputStream(new HadoopIgfsProxyInputStream(is, clientLog, logId));
                }
                else
                    return is;
            }
            else {
                HadoopIgfsStreamDelegate stream = seqReadsBeforePrefetchOverride ?
                    rmtClient.open(path, seqReadsBeforePrefetch) : rmtClient.open(path);

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = IgfsLogger.nextId();

                    clientLog.logOpen(logId, path, mode, bufSize, stream.length());
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opening input stream [thread=" + Thread.currentThread().getName() + ", path=" + path +
                        ", bufSize=" + bufSize + ']');

                HadoopIgfsInputStream igfsIn = new HadoopIgfsInputStream(stream, stream.length(),
                    bufSize, LOG, clientLog, logId);

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened input stream [path=" + path + ", delegate=" + stream + ']');

                return new FSDataInputStream(igfsIn);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public FSDataOutputStream create(Path f, final FsPermission perm, boolean overwrite, int bufSize,
        short replication, long blockSize, Progressable progress) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        OutputStream out = null;

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

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
                    long logId = IgfsLogger.nextId();

                    clientLog.logCreate(logId, path, PROXY, overwrite, bufSize, replication, blockSize);

                    return new FSDataOutputStream(new HadoopIgfsProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                Map<String,String> propMap = permission(perm);

                propMap.put(PROP_PREFER_LOCAL_WRITES, Boolean.toString(preferLocFileWrites));

                // Create stream and close it in the 'finally' section if any sequential operation failed.
                HadoopIgfsStreamDelegate stream = rmtClient.create(path, overwrite, colocateFileWrites,
                    replication, blockSize, propMap);

                assert stream != null;

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = IgfsLogger.nextId();

                    clientLog.logCreate(logId, path, mode, overwrite, bufSize, replication, blockSize);
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened output stream in create [path=" + path + ", delegate=" + stream + ']');

                HadoopIgfsOutputStream igfsOut = new HadoopIgfsOutputStream(stream, LOG, clientLog,
                    logId);

                bufSize = Math.max(64 * 1024, bufSize);

                out = new BufferedOutputStream(igfsOut, bufSize);

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
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

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
                    long logId = IgfsLogger.nextId();

                    clientLog.logAppend(logId, path, PROXY, bufSize); // Don't have stream ID.

                    return new FSDataOutputStream(new HadoopIgfsProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                HadoopIgfsStreamDelegate stream = rmtClient.append(path, false, null);

                assert stream != null;

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = IgfsLogger.nextId();

                    clientLog.logAppend(logId, path, mode, bufSize);
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened output stream in append [path=" + path + ", delegate=" + stream + ']');

                HadoopIgfsOutputStream igfsOut = new HadoopIgfsOutputStream(stream, LOG, clientLog,
                    logId);

                bufSize = Math.max(64 * 1024, bufSize);

                BufferedOutputStream out = new BufferedOutputStream(igfsOut, bufSize);

                return new FSDataOutputStream(out, null, 0);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean rename(Path src, Path dst) throws IOException {
        A.notNull(src, "src");
        A.notNull(dst, "dst");

        enterBusy();

        try {
            IgfsPath srcPath = convert(src);
            IgfsPath dstPath = convert(dst);
            IgfsMode mode = mode(srcPath);

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
                if (clientLog.isLogEnabled())
                    clientLog.logRename(srcPath, mode, dstPath);

                try {
                    rmtClient.rename(srcPath, dstPath);
                }
                catch (IOException ioe) {
                    // Log the exception before rethrowing since it may be ignored:
                    LOG.warn("Failed to rename [srcPath=" + srcPath + ", dstPath=" + dstPath + ", mode=" + mode + ']',
                        ioe);

                    throw ioe;
                }

                return true;
            }
        }
        catch (IOException e) {
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !X.hasCause(e.getCause(), IgfsException.class)))
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
    @SuppressWarnings("unchecked")
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

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
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !X.hasCause(e.getCause(), IgfsException.class)))
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
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            if (mode == PROXY) {
                if (secondaryFs == null) {
                    assert mgmt;

                    return EMPTY_FILE_STATUS;
                }

                FileStatus[] arr = secondaryFs.listStatus(toSecondary(f));

                if (arr == null)
                    throw new FileNotFoundException("File " + f + " does not exist.");

                for (int i = 0; i < arr.length; i++)
                    arr[i] = toPrimary(arr[i]);

                if (clientLog.isLogEnabled()) {
                    String[] fileArr = new String[arr.length];

                    for (int i = 0; i < arr.length; i++)
                        fileArr[i] = arr[i].getPath().toString();

                    clientLog.logListDirectory(path, PROXY, fileArr);
                }

                return arr;
            }
            else {
                Collection<IgfsFile> list = rmtClient.listFiles(path);

                if (list == null)
                    throw new FileNotFoundException("File " + f + " does not exist.");

                List<IgfsFile> files = new ArrayList<>(list);

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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Path getHomeDirectory() {
        Path path = new Path("/user/" + user);

        return path.makeQualified(getUri(), null);
    }

    /** {@inheritDoc} */
    @Override public void setWorkingDirectory(Path newPath) {
        if (newPath == null) {
            Path homeDir = getHomeDirectory();

            if (secondaryFs != null)
                secondaryFs.setWorkingDirectory(toSecondary(homeDir));

            workingDir = homeDir;
        }
        else {
            Path fixedNewPath = fixRelativePart(newPath);

            String res = fixedNewPath.toUri().getPath();

            if (!DFSUtil.isValidName(res))
                throw new IllegalArgumentException("Invalid DFS directory name " + res);

            if (secondaryFs != null)
                secondaryFs.setWorkingDirectory(toSecondary(fixedNewPath));

            workingDir = fixedNewPath;
        }
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workingDir;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean mkdirs(Path f, FsPermission perm) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

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
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                !X.hasCause(e.getCause(), IgfsException.class)))
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
                IgfsFile info = rmtClient.info(convert(f));

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
                IgfsPathSummary sum = rmtClient.contentSummary(convert(f));

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
            IgfsPath path = convert(status.getPath());

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

                List<IgfsBlockLocation> affinity = new ArrayList<>(rmtClient.affinity(path, start, len));

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
        return igfsGrpBlockSize;
    }

    /**
     * Resolve path mode.
     *
     * @param path HDFS path.
     * @return Path mode.
     */
    public IgfsMode mode(Path path) {
        return mode(convert(path));
    }

    /**
     * Resolve path mode.
     *
     * @param path IGFS path.
     * @return Path mode.
     */
    public IgfsMode mode(IgfsPath path) {
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
                throw new IgniteException("Failed to construct secondary file system path from the primary file " +
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
     * Convert IGFS path into Hadoop path.
     *
     * @param path IGFS path.
     * @return Hadoop path.
     */
    private Path convert(IgfsPath path) {
        return new Path(IGFS_SCHEME, uriAuthority, path.toString());
    }

    /**
     * Convert Hadoop path into IGFS path.
     *
     * @param path Hadoop path.
     * @return IGFS path.
     */
    @Nullable private IgfsPath convert(@Nullable Path path) {
        if (path == null)
            return null;

        return path.isAbsolute() ? new IgfsPath(path.toUri().getPath()) :
            new IgfsPath(convert(workingDir), path.toUri().getPath());
    }

    /**
     * Convert IGFS affinity block location into Hadoop affinity block location.
     *
     * @param block IGFS affinity block location.
     * @return Hadoop affinity block location.
     */
    private BlockLocation convert(IgfsBlockLocation block) {
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
     * Convert IGFS file information into Hadoop file status.
     *
     * @param file IGFS file information.
     * @return Hadoop file status.
     */
    @SuppressWarnings("deprecation")
    private FileStatus convert(IgfsFile file) {
        return new FileStatus(
            file.length(),
            file.isDirectory(),
            getDefaultReplication(),
            file.groupBlockSize(),
            file.modificationTime(),
            file.accessTime(),
            permission(file),
            file.property(PROP_USER_NAME, user),
            file.property(PROP_GROUP_NAME, "users"),
            convert(file.path())) {
            @Override public String toString() {
                return "FileStatus [path=" + getPath() + ", isDir=" + isDir() + ", len=" + getLen() +
                    ", mtime=" + getModificationTime() + ", atime=" + getAccessTime() + ']';
            }
        };
    }

    /**
     * Convert Hadoop permission into IGFS file attribute.
     *
     * @param perm Hadoop permission.
     * @return IGFS attributes.
     */
    private Map<String, String> permission(FsPermission perm) {
        if (perm == null)
            perm = FsPermission.getDefault();

        return F.asMap(PROP_PERMISSION, toString(perm));
    }

    /**
     * @param perm Permission.
     * @return String.
     */
    private static String toString(FsPermission perm) {
        return String.format("%04o", perm.toShort());
    }

    /**
     * Convert IGFS file attributes into Hadoop permission.
     *
     * @param file File info.
     * @return Hadoop permission.
     */
    private FsPermission permission(IgfsFile file) {
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
        return S.toString(IgniteHadoopFileSystem.class, this);
    }

    /**
     * Returns the user name this File System is created on behalf of.
     * @return the user name
     */
    public String user() {
        return user;
    }
}