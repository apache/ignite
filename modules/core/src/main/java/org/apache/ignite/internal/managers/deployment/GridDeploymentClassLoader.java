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

package org.apache.ignite.internal.managers.deployment;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.GridBoundedLinkedHashSet;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Class loader that is able to resolve task subclasses and resources
 * by requesting remote node. Every class that could not be resolved
 * by system class loader will be downloaded from given remote node
 * by task deployment identifier. If identifier has been changed on
 * remote node this class will throw exception.
 */
@SuppressWarnings({"CustomClassloader"})
class GridDeploymentClassLoader extends ClassLoader implements GridDeploymentInfo {
    /** Class loader ID. */
    private final IgniteUuid id;

    /** {@code True} for single node deployment. */
    private final boolean singleNode;

    /** Manager registry. */
    @GridToStringExclude
    private final GridKernalContext ctx;

    /** */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Registered nodes. */
    @GridToStringExclude
    private LinkedList<UUID> nodeList;

    /** Node ID -> Loader ID. */
    @GridToStringInclude
    private Map<UUID, IgniteUuid> nodeLdrMap;

    /** */
    @GridToStringExclude
    private final GridDeploymentCommunication comm;

    /** */
    private final String[] p2pExclude;

    /** P2P timeout. */
    private final long p2pTimeout;

    /** Cache of missed resources names. */
    @GridToStringExclude
    private final GridBoundedLinkedHashSet<String> missedRsrcs;

    /** Map of class byte code if it's not available locally. */
    @GridToStringExclude
    private final ConcurrentMap<String, byte[]> byteMap;

    /** User version. */
    private final String usrVer;

    /** Deployment mode. */
    private final DeploymentMode depMode;

    /** {@code True} to omit any output to INFO or higher. */
    private boolean quiet;

    /** */
    private final Object mux = new Object();

    /**
     * Creates a new peer class loader.
     * <p>
     * If there is a security manager, its
     * {@link SecurityManager#checkCreateClassLoader()}
     * method is invoked. This may result in a security exception.
     *
     * @param id Class loader ID.
     * @param usrVer User version.
     * @param depMode Deployment mode.
     * @param singleNode {@code True} for single node.
     * @param ctx Kernal context.
     * @param parent Parent class loader.
     * @param clsLdrId Remote class loader identifier.
     * @param nodeId ID of node that have initiated task.
     * @param comm Communication manager loader will work through.
     * @param p2pTimeout Timeout for class-loading requests.
     * @param log Logger.
     * @param p2pExclude List of P2P loaded packages.
     * @param missedResourcesCacheSize Size of the missed resources cache.
     * @param clsBytesCacheEnabled Flag to enable class byte cache.
     * @param quiet {@code True} to omit output to log.
     * @throws SecurityException If a security manager exists and its
     *      {@code checkCreateClassLoader} method doesn't allow creation
     *      of a new class loader.
     */
    GridDeploymentClassLoader(
        IgniteUuid id,
        String usrVer,
        DeploymentMode depMode,
        boolean singleNode,
        GridKernalContext ctx,
        ClassLoader parent,
        IgniteUuid clsLdrId,
        UUID nodeId,
        GridDeploymentCommunication comm,
        long p2pTimeout,
        IgniteLogger log,
        String[] p2pExclude,
        int missedResourcesCacheSize,
        boolean clsBytesCacheEnabled,
        boolean quiet) throws SecurityException {
        super(parent);

        assert id != null;
        assert depMode != null;
        assert ctx != null;
        assert comm != null;
        assert p2pTimeout > 0;
        assert log != null;
        assert clsLdrId != null;
        assert nodeId.equals(clsLdrId.globalId());

        this.id = id;
        this.usrVer = usrVer;
        this.depMode = depMode;
        this.singleNode = singleNode;
        this.ctx = ctx;
        this.comm = comm;
        this.p2pTimeout = p2pTimeout;
        this.log = log;
        this.p2pExclude = p2pExclude;

        nodeList = new LinkedList<>();

        nodeList.add(nodeId);

        Map<UUID, IgniteUuid> map = U.newHashMap(1);

        map.put(nodeId, clsLdrId);

        nodeLdrMap = singleNode ? Collections.unmodifiableMap(map) : map;

        missedRsrcs = missedResourcesCacheSize > 0 ?
            new GridBoundedLinkedHashSet<String>(missedResourcesCacheSize) : null;

        byteMap = clsBytesCacheEnabled ? new ConcurrentHashMap8<String, byte[]>() : null;

        this.quiet = quiet;
    }

    /**
     * Creates a new peer class loader.
     * <p>
     * If there is a security manager, its
     * {@link SecurityManager#checkCreateClassLoader()}
     * method is invoked. This may result in a security exception.
     *
     * @param id Class loader ID.
     * @param usrVer User version.
     * @param depMode Deployment mode.
     * @param singleNode {@code True} for single node.
     * @param ctx Kernal context.
     * @param parent Parent class loader.
     * @param participants Participating nodes class loader map.
     * @param comm Communication manager loader will work through.
     * @param p2pTimeout Timeout for class-loading requests.
     * @param log Logger.
     * @param p2pExclude List of P2P loaded packages.
     * @param missedResourcesCacheSize Size of the missed resources cache.
     * @param clsBytesCacheEnabled Flag to enable class byte cache.
     * @param quiet {@code True} to omit output to log.
     * @throws SecurityException If a security manager exists and its
     *      {@code checkCreateClassLoader} method doesn't allow creation
     *      of a new class loader.
     */
    GridDeploymentClassLoader(
        IgniteUuid id,
        String usrVer,
        DeploymentMode depMode,
        boolean singleNode,
        GridKernalContext ctx,
        ClassLoader parent,
        Map<UUID, IgniteUuid> participants,
        GridDeploymentCommunication comm,
        long p2pTimeout,
        IgniteLogger log,
        String[] p2pExclude,
        int missedResourcesCacheSize,
        boolean clsBytesCacheEnabled,
        boolean quiet) throws SecurityException {
        super(parent);

        assert id != null;
        assert depMode != null;
        assert ctx != null;
        assert comm != null;
        assert p2pTimeout > 0;
        assert log != null;
        assert participants != null;

        this.id = id;
        this.usrVer = usrVer;
        this.depMode = depMode;
        this.singleNode = singleNode;
        this.ctx = ctx;
        this.comm = comm;
        this.p2pTimeout = p2pTimeout;
        this.log = log;
        this.p2pExclude = p2pExclude;

        nodeList = new LinkedList<>(participants.keySet());

        nodeLdrMap = new HashMap<>(participants);

        missedRsrcs = missedResourcesCacheSize > 0 ?
            new GridBoundedLinkedHashSet<String>(missedResourcesCacheSize) : null;

        byteMap = clsBytesCacheEnabled ? new ConcurrentHashMap8<String, byte[]>() : null;

        this.quiet = quiet;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid classLoaderId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public DeploymentMode deployMode() {
        return depMode;
    }

    /** {@inheritDoc} */
    @Override public String userVersion() {
        return usrVer;
    }

    /** {@inheritDoc} */
    @Override public boolean localDeploymentOwner() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long sequenceNumber() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, IgniteUuid> participants() {
        synchronized (mux) {
            return new HashMap<>(nodeLdrMap);
        }
    }

    /**
     * Adds new node and remote class loader id to this class loader.
     * Class loader will ask all associated nodes for the class/resource
     * until find it.
     *
     * @param nodeId Participating node ID.
     * @param ldrId Participating class loader id.
     */
    void register(UUID nodeId, IgniteUuid ldrId) {
        assert nodeId != null;
        assert ldrId != null;
        assert nodeId.equals(ldrId.globalId());
        assert !singleNode;

        synchronized (mux) {
            if (missedRsrcs != null)
                missedRsrcs.clear();

            /*
             * We need to put passed in node into the
             * first position in the list.
             */

            // 1. Remove passed in node if any.
            nodeList.remove(nodeId);

            // 2. Add passed in node to the first position.
            nodeList.addFirst(nodeId);

            // 3. Put to map.
            nodeLdrMap.put(nodeId, ldrId);
        }
    }

    /**
     * Remove remote node and remote class loader id associated with it from
     * internal map.
     *
     * @param nodeId Participating node ID.
     * @return Removed class loader ID.
     */
    @Nullable
    IgniteUuid unregister(UUID nodeId) {
        assert nodeId != null;

        synchronized (mux) {
            nodeList.remove(nodeId);

            return nodeLdrMap.remove(nodeId);
        }
    }

    /**
     * @return Registered nodes.
     */
    Collection<UUID> registeredNodeIds() {
        synchronized (mux) {
            return new ArrayList<>(nodeList);
        }
    }

    /**
     * @return Registered class loader IDs.
     */
    Collection<IgniteUuid> registeredClassLoaderIds() {
        Collection<IgniteUuid> ldrIds = new LinkedList<>();

        synchronized (mux) {
            for (IgniteUuid ldrId : nodeLdrMap.values())
                ldrIds.add(ldrId);
        }

        return ldrIds;
    }

    /**
     * @param nodeId Node ID.
     * @return Class loader ID for node ID.
     */
    IgniteUuid registeredClassLoaderId(UUID nodeId) {
        synchronized (mux) {
            return nodeLdrMap.get(nodeId);
        }
    }

    /**
     * Checks if node is participating in deployment.
     *
     * @param nodeId Node ID to check.
     * @param ldrId Class loader ID.
     * @return {@code True} if node is participating in deployment.
     */
    boolean hasRegisteredNode(UUID nodeId, IgniteUuid ldrId) {
        assert nodeId != null;
        assert ldrId != null;

        IgniteUuid ldrId0;

        synchronized (mux) {
            ldrId0 = nodeLdrMap.get(nodeId);
        }

        return ldrId0 != null && ldrId0.equals(ldrId);
    }

    /**
     * @return {@code True} if class loader has registered nodes.
     */
    boolean hasRegisteredNodes() {
        synchronized (mux) {
            return !nodeList.isEmpty();
        }
    }

    /**
     * @param name Name of the class.
     * @return {@code True} if locally excluded.
     */
    private boolean isLocallyExcluded(String name) {
        if (p2pExclude != null) {
            for (String path : p2pExclude) {
                // Remove star (*) at the end.
                if (path.endsWith("*"))
                    path = path.substring(0, path.length() - 1);

                if (name.startsWith(path))
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public Class<?> loadClass(String name) throws ClassNotFoundException {
        assert !Thread.holdsLock(mux);

        // Check if we have package name on list of P2P loaded.
        // ComputeJob must be always loaded locally to avoid
        // any possible class casting issues.
        Class<?> cls = null;

        try {
            if (!"org.apache.ignite.compute.ComputeJob".equals(name)) {
                if (isLocallyExcluded(name))
                    // P2P loaded class.
                    cls = p2pLoadClass(name, true);
            }

            if (cls == null)
                cls = loadClass(name, true);
        }
        catch (ClassNotFoundException e) {
            throw e;
        }
        // Catch Throwable to secure against any errors resulted from
        // corrupted class definitions or other user errors.
        catch (Exception e) {
            throw new ClassNotFoundException("Failed to load class due to unexpected error: " + name, e);
        }

        return cls;
    }

    /**
     * Loads the class with the specified binary name. The
     * default implementation of this method searches for classes in the
     * following order:
     * <p>
     * <ol>
     * <li> Invoke {@link #findLoadedClass(String)} to check if the class
     * has already been loaded. </li>
     * <li>Invoke the {@link #findClass(String)} method to find the class.</li>
     * </ol>
     * <p> If the class was found using the above steps, and the
     * {@code resolve} flag is true, this method will then invoke the {@link
     * #resolveClass(Class)} method on the resulting {@code Class} object.
     *
     * @param name The binary name of the class.
     * @param resolve If {@code true} then resolve the class.
     * @return The resulting {@code Class} object.
     * @throws ClassNotFoundException If the class could not be found
     */
    @Nullable private Class<?> p2pLoadClass(String name, boolean resolve) throws ClassNotFoundException {
        assert !Thread.holdsLock(mux);

        // First, check if the class has already been loaded.
        Class<?> cls = findLoadedClass(name);

        if (cls == null)
            cls = findClass(name);

        if (resolve)
            resolveClass(cls);

        return cls;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
        assert !Thread.holdsLock(mux);

        if (!isLocallyExcluded(name)) {
            // This is done for URI deployment in which case the parent loader
            // does not have the requested resource, but it is still locally
            // available.
            GridDeployment dep = ctx.deploy().getLocalDeployment(name);

            if (dep != null) {
                if (log.isDebugEnabled())
                    log.debug("Found class in local deployment [cls=" + name + ", dep=" + dep + ']');

                return dep.deployedClass(name);
            }
        }

        String path = U.classNameToResourceName(name);

        GridByteArrayList byteSrc = sendClassRequest(name, path);

        synchronized (this) {
            Class<?> cls = findLoadedClass(name);

            if (cls == null) {
                if (byteMap != null)
                    byteMap.put(path, byteSrc.array());

                cls = defineClass(name, byteSrc.internalArray(), 0, byteSrc.size());

                /* Define package in classloader. See URLClassLoader.defineClass(). */
                int i = name.lastIndexOf('.');

                if (i != -1) {
                    String pkgName = name.substring(0, i);

                    if (getPackage(pkgName) == null)
                         // Too much nulls is normal because we don't have package's meta info.
                         definePackage(pkgName, null, null, null, null, null, null, null);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Loaded class [cls=" + name + ", ldr=" + this + ']');

            return cls;
        }
    }

    /**
     * Computes end time based on timeout value passed in.
     *
     * @param timeout Timeout.
     * @return End time.
     */
    private long computeEndTime(long timeout) {
        long endTime = U.currentTimeMillis() + timeout;

        // Account for overflow.
        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        return endTime;
    }

    /**
     * Sends class-loading request to all nodes associated with this class loader.
     *
     * @param name Class name.
     * @param path Class path.
     * @return Class byte source.
     * @throws ClassNotFoundException If class was not found.
     */
    private GridByteArrayList sendClassRequest(String name, String path) throws ClassNotFoundException {
        assert !Thread.holdsLock(mux);

        long endTime = computeEndTime(p2pTimeout);

        Collection<UUID> nodeListCp;
        Map<UUID, IgniteUuid> nodeLdrMapCp;

        synchronized (mux) {
            // Skip requests for the previously missed classes.
            if (missedRsrcs != null && missedRsrcs.contains(path))
                throw new ClassNotFoundException("Failed to peer load class [class=" + name + ", nodeClsLdrIds=" +
                    nodeLdrMap + ", parentClsLoader=" + getParent() + ']');

            // If single-node mode, then node cannot change and we simply reuse list and map.
            // Otherwise, make copies that can be used outside synchronization.
            nodeListCp = singleNode ? nodeList : new LinkedList<>(nodeList);
            nodeLdrMapCp = singleNode ? nodeLdrMap : new HashMap<>(nodeLdrMap);
        }

        IgniteCheckedException err = null;

        for (UUID nodeId : nodeListCp) {
            if (nodeId.equals(ctx.discovery().localNode().id()))
                // Skip local node as it is already used as parent class loader.
                continue;

            IgniteUuid ldrId = nodeLdrMapCp.get(nodeId);

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Found inactive node in class loader (will skip): " + nodeId);

                continue;
            }

            try {
                GridDeploymentResponse res = comm.sendResourceRequest(path, ldrId, node, endTime);

                if (res == null) {
                    String msg = "Failed to send class-loading request to node (is node alive?) [node=" +
                        node.id() + ", clsName=" + name + ", clsPath=" + path + ", clsLdrId=" + ldrId +
                        ", parentClsLdr=" + getParent() + ']';

                    if (!quiet)
                        U.warn(log, msg);
                    else if (log.isDebugEnabled())
                        log.debug(msg);

                    err = new IgniteCheckedException(msg);

                    continue;
                }

                if (res.success())
                    return res.byteSource();

                // In case of shared resources/classes all nodes should have it.
                if (log.isDebugEnabled())
                    log.debug("Failed to find class on remote node [class=" + name + ", nodeId=" + node.id() +
                        ", clsLdrId=" + ldrId + ", reason=" + res.errorMessage() + ']');

                synchronized (mux) {
                    if (missedRsrcs != null)
                        missedRsrcs.add(path);
                }

                throw new ClassNotFoundException("Failed to peer load class [class=" + name + ", nodeClsLdrs=" +
                    nodeLdrMapCp + ", parentClsLoader=" + getParent() + ", reason=" + res.errorMessage() + ']');
            }
            catch (IgniteCheckedException e) {
                // This thread should be interrupted again in communication if it
                // got interrupted. So we assume that thread can be interrupted
                // by processing cancellation request.
                if (Thread.currentThread().isInterrupted()) {
                    if (!quiet)
                        U.error(log, "Failed to find class probably due to task/job cancellation: " + name, e);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to find class probably due to task/job cancellation [name=" + name +
                            ", err=" + e + ']');
                }
                else {
                    if (!quiet)
                        U.warn(log, "Failed to send class-loading request to node (is node alive?) [node=" +
                            node.id() + ", clsName=" + name + ", clsPath=" + path + ", clsLdrId=" + ldrId +
                            ", parentClsLdr=" + getParent() + ", err=" + e + ']');
                    else if (log.isDebugEnabled())
                        log.debug("Failed to send class-loading request to node (is node alive?) [node=" +
                            node.id() + ", clsName=" + name + ", clsPath=" + path + ", clsLdrId=" + ldrId +
                            ", parentClsLdr=" + getParent() + ", err=" + e + ']');

                    err = e;
                }
            }
        }

        throw new ClassNotFoundException("Failed to peer load class [class=" + name + ", nodeClsLdrs=" +
            nodeLdrMapCp + ", parentClsLoader=" + getParent() + ']', err);
    }

    /** {@inheritDoc} */
    @Nullable @Override public InputStream getResourceAsStream(String name) {
        assert !Thread.holdsLock(mux);

        if (byteMap != null && name.endsWith(".class")) {
            byte[] bytes = byteMap.get(name);

            if (bytes != null) {
                if (log.isDebugEnabled())
                    log.debug("Got class definition from byte code cache: " + name);

                return new ByteArrayInputStream(bytes);
            }
        }

        InputStream in = ClassLoader.getSystemResourceAsStream(name);

        if (in == null)
            in = super.getResourceAsStream(name);

        // Most probably, this is initiated by GridUtils.getUserVersion().
        // No point to send request.
        if ("META-INF/services/org.apache.commons.logging.LogFactory".equalsIgnoreCase(name)) {
            if (log.isDebugEnabled())
                log.debug("Denied sending remote request for META-INF/services/org.apache.commons.logging.LogFactory.");

            return null;
        }

        if (in == null)
            in = sendResourceRequest(name);

        return in;
    }

    /**
     * Sends resource request to all remote nodes associated with this class loader.
     *
     * @param name Resource name.
     * @return InputStream for resource or {@code null} if resource could not be found.
     */
    @Nullable private InputStream sendResourceRequest(String name) {
        assert !Thread.holdsLock(mux);

        long endTime = computeEndTime(p2pTimeout);

        Collection<UUID> nodeListCp;
        Map<UUID, IgniteUuid> nodeLdrMapCp;

        synchronized (mux) {
            // Skip requests for the previously missed classes.
            if (missedRsrcs != null && missedRsrcs.contains(name))
                return null;

            // If single-node mode, then node cannot change and we simply reuse list and map.
            // Otherwise, make copies that can be used outside synchronization.
            nodeListCp = singleNode ? nodeList : new LinkedList<>(nodeList);
            nodeLdrMapCp = singleNode ? nodeLdrMap : new HashMap<>(nodeLdrMap);
        }

        for (UUID nodeId : nodeListCp) {
            if (nodeId.equals(ctx.discovery().localNode().id()))
                // Skip local node as it is already used as parent class loader.
                continue;

            IgniteUuid ldrId = nodeLdrMapCp.get(nodeId);

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Found inactive node in class loader (will skip): " + nodeId);

                continue;
            }

            try {
                // Request is sent with timeout that is why we can use synchronization here.
                GridDeploymentResponse res = comm.sendResourceRequest(name, ldrId, node, endTime);

                if (res == null) {
                    U.warn(log, "Failed to get resource from node (is node alive?) [nodeId=" +
                        node.id() + ", clsLdrId=" + ldrId + ", resName=" +
                        name + ", parentClsLdr=" + getParent() + ']');
                }
                else if (!res.success()) {
                    synchronized (mux) {
                        // Cache unsuccessfully loaded resource.
                        if (missedRsrcs != null)
                            missedRsrcs.add(name);
                    }

                    // Some frameworks like Spring often ask for the resources
                    // just in case - none will happen if there are no such
                    // resources. So we print out INFO level message.
                    if (!quiet) {
                        if (log.isInfoEnabled())
                            log.info("Failed to get resource from node [nodeId=" +
                                node.id() + ", clsLdrId=" + ldrId + ", resName=" +
                                name + ", parentClsLdr=" + getParent() + ", msg=" + res.errorMessage() + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Failed to get resource from node [nodeId=" +
                            node.id() + ", clsLdrId=" + ldrId + ", resName=" +
                            name + ", parentClsLdr=" + getParent() + ", msg=" + res.errorMessage() + ']');

                    // Do not ask other nodes in case of shared mode all of them should have the resource.
                    return null;
                }
                else {
                    return new ByteArrayInputStream(res.byteSource().internalArray(), 0,
                        res.byteSource().size());
                }
            }
            catch (IgniteCheckedException e) {
                // This thread should be interrupted again in communication if it
                // got interrupted. So we assume that thread can be interrupted
                // by processing cancellation request.
                if (Thread.currentThread().isInterrupted()) {
                    if (!quiet)
                        U.error(log, "Failed to get resource probably due to task/job cancellation: " + name, e);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to get resource probably due to task/job cancellation: " + name);
                }
                else {
                    if (!quiet)
                        U.warn(log, "Failed to get resource from node (is node alive?) [nodeId=" +
                            node.id() + ", clsLdrId=" + ldrId + ", resName=" +
                            name + ", parentClsLdr=" + getParent() + ", err=" + e + ']');
                    else if (log.isDebugEnabled())
                        log.debug("Failed to get resource from node (is node alive?) [nodeId=" +
                            node.id() + ", clsLdrId=" + ldrId + ", resName=" +
                            name + ", parentClsLdr=" + getParent() + ", err=" + e + ']');
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        synchronized (mux) {
            return S.toString(GridDeploymentClassLoader.class, this);
        }
    }
}