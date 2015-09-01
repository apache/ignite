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

package org.apache.ignite.hadoop.fs.v2;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.processors.hadoop.SecondaryFileSystemProvider;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsEndpoint;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsInputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsOutputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProxyInputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsProxyOutputStream;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsStreamDelegate;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsWrapper;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsModeResolver;
import org.apache.ignite.internal.processors.igfs.IgfsPaths;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_BATCH_SIZE;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_DIR;
import static org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.getFsHadoopUser;
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
 * {@code IGFS} Hadoop 2.x file system driver over file system API. To use
 * {@code IGFS} as Hadoop file system, you should configure this class
 * in Hadoop's {@code core-site.xml} as follows:
 * <pre name="code" class="xml">
 *  &lt;property&gt;
 *      &lt;name&gt;fs.default.name&lt;/name&gt;
 *      &lt;value&gt;igfs://ipc&lt;/value&gt;
 *  &lt;/property&gt;
 *
 *  &lt;property&gt;
 *      &lt;name&gt;fs.igfs.impl&lt;/name&gt;
 *      &lt;value&gt;org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem&lt;/value&gt;
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
public class IgniteHadoopFileSystem extends AbstractFileSystem implements Closeable {
    /** Logger. */
    private static final Log LOG = LogFactory.getLog(IgniteHadoopFileSystem.class);

    /** Ensures that close routine is invoked at most once. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Grid remote client. */
    private HadoopIgfsWrapper rmtClient;

    /** The name of the user this File System created on behalf of. */
    private final String user;

    /** Working directory. */
    private IgfsPath workingDir;

    /** URI. */
    private final URI uri;

    /** Authority. */
    private String uriAuthority;

    /** Client logger. */
    private IgfsLogger clientLog;

    /** Server block size. */
    private long grpBlockSize;

    /** Default replication factor. */
    private short dfltReplication;

    /** Secondary URI string. */
    private URI secondaryUri;

    /** Mode resolver. */
    private IgfsModeResolver modeRslvr;

    /** Secondary file system instance. */
    private AbstractFileSystem secondaryFs;

    /** Whether custom sequential reads before prefetch value is provided. */
    private boolean seqReadsBeforePrefetchOverride;

    /** Custom-provided sequential reads before prefetch. */
    private int seqReadsBeforePrefetch;

    /** Flag that controls whether file writes should be colocated on data node. */
    private boolean colocateFileWrites;

    /** Prefer local writes. */
    private boolean preferLocFileWrites;

    /**
     * @param name URI for file system.
     * @param cfg Configuration.
     * @throws URISyntaxException if name has invalid syntax.
     * @throws IOException If initialization failed.
     */
    public IgniteHadoopFileSystem(URI name, Configuration cfg) throws URISyntaxException, IOException {
        super(HadoopIgfsEndpoint.normalize(name), IGFS_SCHEME, false, -1);

        uri = name;

        user = getFsHadoopUser();

        try {
            initialize(name, cfg);
        }
        catch (IOException e) {
            // Close client if exception occurred.
            if (rmtClient != null)
                rmtClient.close(false);

            throw e;
        }

        workingDir = new IgfsPath("/user/" + user);
    }

    /** {@inheritDoc} */
    @Override public void checkPath(Path path) {
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

    /**
     * Public setter that can be used by direct users of FS or Visor.
     *
     * @param colocateFileWrites Whether all ongoing file writes should be colocated.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void colocateFileWrites(boolean colocateFileWrites) {
        this.colocateFileWrites = colocateFileWrites;
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
     * @param name URI passed to constructor.
     * @param cfg Configuration passed to constructor.
     * @throws IOException If initialization failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void initialize(URI name, Configuration cfg) throws IOException {
        enterBusy();

        try {
            if (rmtClient != null)
                throw new IOException("File system is already initialized: " + rmtClient);

            A.notNull(name, "name");
            A.notNull(cfg, "cfg");

            if (!IGFS_SCHEME.equals(name.getScheme()))
                throw new IOException("Illegal file system URI [expected=" + IGFS_SCHEME +
                    "://[name]/[optional_path], actual=" + name + ']');

            uriAuthority = name.getAuthority();

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

            grpBlockSize = handshake.blockSize();

            IgfsPaths paths = handshake.secondaryPaths();

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

            if (!initSecondary && paths.pathModes() != null) {
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

                    secondaryFs = secProvider.createAbstractFileSystem(user);

                    secondaryUri = secProvider.uri();
                }
                catch (IOException e) {
                    throw new IOException("Failed to connect to the secondary file system: " + secUri, e);
                }
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (closeGuard.compareAndSet(false, true)) {
            if (rmtClient == null)
                return;

            rmtClient.close(false);

            if (clientLog.isLogEnabled())
                clientLog.close();

            // Reset initialized resources.
            rmtClient = null;
        }
    }

    /** {@inheritDoc} */
    @Override public URI getUri() {
        return uri;
    }

    /** {@inheritDoc} */
    @Override public int getUriDefaultPort() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public FsServerDefaults getServerDefaults() throws IOException {
        return new FsServerDefaults(grpBlockSize, (int)grpBlockSize, (int)grpBlockSize, dfltReplication, 64 * 1024,
            false, 0, DataChecksum.Type.NULL);
    }

    /** {@inheritDoc} */
    @Override public boolean setReplication(Path f, short replication) throws IOException {
        return mode(f) == PROXY && secondaryFs.setReplication(f, replication);
    }

    /** {@inheritDoc} */
    @Override public void setTimes(Path f, long mtime, long atime) throws IOException {
        if (mode(f) == PROXY)
            secondaryFs.setTimes(f, mtime, atime);
        else {
            if (mtime == -1 && atime == -1)
                return;

            rmtClient.setTimes(convert(f), atime, mtime);
        }
    }

    /** {@inheritDoc} */
    @Override public FsStatus getFsStatus() throws IOException {
        IgfsStatus status = rmtClient.fsStatus();

        return new FsStatus(status.spaceTotal(), status.spaceUsed(), status.spaceTotal() - status.spaceUsed());
    }

    /** {@inheritDoc} */
    @Override public void setPermission(Path p, FsPermission perm) throws IOException {
        enterBusy();

        try {
            A.notNull(p, "p");

            if (mode(p) == PROXY)
                secondaryFs.setPermission(toSecondary(p), perm);
            else {
                if (rmtClient.update(convert(p), permission(perm)) == null)
                    throw new IOException("Failed to set file permission (file not found?)" +
                        " [path=" + p + ", perm=" + perm + ']');
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void setOwner(Path p, String usr, String grp) throws IOException {
        A.notNull(p, "p");
        A.notNull(usr, "username");
        A.notNull(grp, "grpName");

        enterBusy();

        try {
            if (mode(p) == PROXY)
                secondaryFs.setOwner(toSecondary(p), usr, grp);
            else if (rmtClient.update(convert(p), F.asMap(PROP_USER_NAME, usr, PROP_GROUP_NAME, grp)) == null)
                throw new IOException("Failed to set file permission (file not found?)" +
                    " [path=" + p + ", username=" + usr + ", grpName=" + grp + ']');
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
            IgfsMode mode = modeRslvr.resolveMode(path);

            if (mode == PROXY) {
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
    @Override public FSDataOutputStream createInternal(
        Path f,
        EnumSet<CreateFlag> flag,
        FsPermission perm,
        int bufSize,
        short replication,
        long blockSize,
        Progressable progress,
        Options.ChecksumOpt checksumOpt,
        boolean createParent
    ) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
        boolean append = flag.contains(CreateFlag.APPEND);
        boolean create = flag.contains(CreateFlag.CREATE);

        OutputStream out = null;

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = modeRslvr.resolveMode(path);

            if (LOG.isDebugEnabled())
                LOG.debug("Opening output stream in create [thread=" + Thread.currentThread().getName() + "path=" +
                    path + ", overwrite=" + overwrite + ", bufSize=" + bufSize + ']');

            if (mode == PROXY) {
                FSDataOutputStream os = secondaryFs.createInternal(toSecondary(f), flag, perm, bufSize,
                    replication, blockSize, progress, checksumOpt, createParent);

                if (clientLog.isLogEnabled()) {
                    long logId = IgfsLogger.nextId();

                    if (append)
                        clientLog.logAppend(logId, path, PROXY, bufSize); // Don't have stream ID.
                    else
                        clientLog.logCreate(logId, path, PROXY, overwrite, bufSize, replication, blockSize);

                    return new FSDataOutputStream(new HadoopIgfsProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                Map<String, String> permMap = F.asMap(PROP_PERMISSION, toString(perm),
                    PROP_PREFER_LOCAL_WRITES, Boolean.toString(preferLocFileWrites));

                // Create stream and close it in the 'finally' section if any sequential operation failed.
                HadoopIgfsStreamDelegate stream;

                long logId = -1;

                if (append) {
                    stream = rmtClient.append(path, create, permMap);

                    if (clientLog.isLogEnabled()) {
                        logId = IgfsLogger.nextId();

                        clientLog.logAppend(logId, path, mode, bufSize);
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("Opened output stream in append [path=" + path + ", delegate=" + stream + ']');
                }
                else {
                    stream = rmtClient.create(path, overwrite, colocateFileWrites, replication, blockSize,
                        permMap);

                    if (clientLog.isLogEnabled()) {
                        logId = IgfsLogger.nextId();

                        clientLog.logCreate(logId, path, mode, overwrite, bufSize, replication, blockSize);
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("Opened output stream in create [path=" + path + ", delegate=" + stream + ']');
                }

                assert stream != null;

                HadoopIgfsOutputStream igfsOut = new HadoopIgfsOutputStream(stream, LOG,
                    clientLog, logId);

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
    @Override public boolean supportsSymlinks() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void renameInternal(Path src, Path dst) throws IOException {
        A.notNull(src, "src");
        A.notNull(dst, "dst");

        enterBusy();

        try {
            IgfsPath srcPath = convert(src);
            IgfsPath dstPath = convert(dst);
            Set<IgfsMode> childrenModes = modeRslvr.resolveChildrenModes(srcPath);

            if (childrenModes.contains(PROXY)) {
                if (clientLog.isLogEnabled())
                    clientLog.logRename(srcPath, PROXY, dstPath);

                secondaryFs.renameInternal(toSecondary(src), toSecondary(dst));
            }
            else {
                if (clientLog.isLogEnabled())
                    clientLog.logRename(srcPath, modeRslvr.resolveMode(srcPath), dstPath);

                rmtClient.rename(srcPath, dstPath);
            }

        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = modeRslvr.resolveMode(path);
            Set<IgfsMode> childrenModes = modeRslvr.resolveChildrenModes(path);

            if (childrenModes.contains(PROXY)) {
                if (clientLog.isLogEnabled())
                    clientLog.logDelete(path, PROXY, recursive);

                return secondaryFs.delete(toSecondary(f), recursive);
            }

            boolean res = rmtClient.delete(path, recursive);

            if (clientLog.isLogEnabled())
                clientLog.logDelete(path, mode, recursive);

            return res;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
        // Checksum has effect for secondary FS only.
        if (secondaryFs != null)
            secondaryFs.setVerifyChecksum(verifyChecksum);
    }

    /** {@inheritDoc} */
    @Override public FileChecksum getFileChecksum(Path f) throws IOException {
        if (mode(f) == PROXY)
            return secondaryFs.getFileChecksum(f);

        return null;
    }

    /** {@inheritDoc} */
    @Override public FileStatus[] listStatus(Path f) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = modeRslvr.resolveMode(path);

            if (mode == PROXY) {
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
    @Override public void mkdir(Path f, FsPermission perm, boolean createParent) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = modeRslvr.resolveMode(path);

            if (mode == PROXY) {
                if (clientLog.isLogEnabled())
                    clientLog.logMakeDirectory(path, PROXY);

                secondaryFs.mkdir(toSecondary(f), perm, createParent);
            }
            else {
                rmtClient.mkdirs(path, permission(perm));

                if (clientLog.isLogEnabled())
                    clientLog.logMakeDirectory(path, mode);
            }
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
            if (mode(f) == PROXY)
                return toPrimary(secondaryFs.getFileStatus(toSecondary(f)));
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
    @Override public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
        A.notNull(path, "path");

        IgfsPath igfsPath = convert(path);

        enterBusy();

        try {
            if (modeRslvr.resolveMode(igfsPath) == PROXY)
                return secondaryFs.getFileBlockLocations(path, start, len);
            else {
                long now = System.currentTimeMillis();

                List<IgfsBlockLocation> affinity = new ArrayList<>(
                    rmtClient.affinity(igfsPath, start, len));

                BlockLocation[] arr = new BlockLocation[affinity.size()];

                for (int i = 0; i < arr.length; i++)
                    arr[i] = convert(affinity.get(i));

                if (LOG.isDebugEnabled())
                    LOG.debug("Fetched file locations [path=" + path + ", fetchTime=" +
                        (System.currentTimeMillis() - now) + ", locations=" + Arrays.asList(arr) + ']');

                return arr;
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Resolve path mode.
     *
     * @param path HDFS path.
     * @return Path mode.
     */
    public IgfsMode mode(Path path) {
        return modeRslvr.resolveMode(convert(path));
    }

    /**
     * Convert the given path to path acceptable by the primary file system.
     *
     * @param path Path.
     * @return Primary file system path.
     */
    private Path toPrimary(Path path) {
        return convertPath(path, getUri());
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
    private FileStatus toPrimary(FileStatus status) {
        return status != null ? new FileStatus(status.getLen(), status.isDirectory(), status.getReplication(),
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
    @Nullable private IgfsPath convert(Path path) {
        if (path == null)
            return null;

        return path.isAbsolute() ? new IgfsPath(path.toUri().getPath()) :
            new IgfsPath(workingDir, path.toUri().getPath());
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
    private FileStatus convert(IgfsFile file) {
        return new FileStatus(
            file.length(),
            file.isDirectory(),
            dfltReplication,
            file.groupBlockSize(),
            file.modificationTime(),
            file.accessTime(),
            permission(file),
            file.property(PROP_USER_NAME, user),
            file.property(PROP_GROUP_NAME, "users"),
            convert(file.path())) {
            @Override public String toString() {
                return "FileStatus [path=" + getPath() + ", isDir=" + isDirectory() + ", len=" + getLen() + "]";
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