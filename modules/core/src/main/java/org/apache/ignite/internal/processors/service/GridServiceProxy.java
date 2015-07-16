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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.services.*;
import org.jsr166.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.GridClosureCallMode.*;

/**
 * Wrapper for making {@link org.apache.ignite.services.Service} class proxies.
 */
class GridServiceProxy<T> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Proxy object. */
    private final T proxy;

    /** Grid projection. */
    private final ClusterGroup prj;

    /** Kernal context. */
    @GridToStringExclude
    private final GridKernalContext ctx;

    /** Remote node to use for proxy invocation. */
    private final AtomicReference<ClusterNode> rmtNode = new AtomicReference<>();

    /** {@code True} if projection includes local node. */
    private boolean hasLocNode;

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param svc Service type class.
     * @param sticky Whether multi-node request should be done.
     * @param ctx Context.
     */
    @SuppressWarnings("unchecked")
    GridServiceProxy(ClusterGroup prj,
        String name,
        Class<? super T> svc,
        boolean sticky,
        GridKernalContext ctx)
    {
        this.prj = prj;
        this.ctx = ctx;
        hasLocNode = hasLocalNode(prj);

        log = ctx.log(getClass());

        proxy = (T)Proxy.newProxyInstance(
            svc.getClassLoader(),
            new Class[] {svc},
            new ProxyInvocationHandler(name, sticky)
        );
    }

    /**
     * @param prj Grid nodes projection.
     * @return Whether given projection contains any local node.
     */
    private boolean hasLocalNode(ClusterGroup prj) {
        for (ClusterNode n : prj.nodes()) {
            if (n.isLocal())
                return true;
        }

        return false;
    }

    /**
     * @return Proxy object for a given instance.
     */
    T proxy() {
        return proxy;
    }

    /**
     * Invocation handler for service proxy.
     */
    private class ProxyInvocationHandler implements InvocationHandler {
        /** Service name. */
        private final String name;

        /** Whether multi-node request should be done. */
        private final boolean sticky;

        /**
         * @param name Name.
         * @param sticky Sticky.
         */
        private ProxyInvocationHandler(String name, boolean sticky) {
            this.name = name;
            this.sticky = sticky;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override public Object invoke(Object proxy, final Method mtd, final Object[] args) {
            while (true) {
                ClusterNode node = null;

                try {
                    node = nodeForService(name, sticky);

                    if (node == null)
                        throw new IgniteException("Failed to find deployed service: " + name);

                    // If service is deployed locally, then execute locally.
                    if (node.isLocal()) {
                        ServiceContextImpl svcCtx = ctx.service().serviceContext(name);

                        if (svcCtx != null)
                            return mtd.invoke(svcCtx.service(), args);
                    }
                    else {
                        // Execute service remotely.
                        return ctx.closure().callAsyncNoFailover(
                            BALANCE,
                            new ServiceProxyCallable(mtd.getName(), name, mtd.getParameterTypes(), args),
                            Collections.singleton(node),
                            false
                        ).get();
                    }
                }
                catch (GridServiceNotFoundException | ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Service was not found or topology changed (will retry): " + e.getMessage());
                }
                catch (RuntimeException | Error e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    throw U.convertException(e);
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }

                // If we are here, that means that service was not found
                // or topology was changed. In this case, we erase the
                // previous sticky node and try again.
                rmtNode.compareAndSet(node, null);

                // Add sleep between retries to avoid busy-wait loops.
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }
            }
        }

        /**
         * @param sticky Whether multi-node request should be done.
         * @param name Service name.
         * @return Node with deployed service or {@code null} if there is no such node.
         */
        private ClusterNode nodeForService(String name, boolean sticky) {
            do { // Repeat if reference to remote node was changed.
                if (sticky) {
                    ClusterNode curNode = rmtNode.get();

                    if (curNode != null)
                        return curNode;

                    curNode = randomNodeForService(name);

                    if (curNode == null)
                        return null;

                    if (rmtNode.compareAndSet(null, curNode))
                        return curNode;
                }
                else
                    return randomNodeForService(name);
            }
            while (true);
        }

        /**
         * @param name Service name.
         * @return Local node if it has a given service deployed or randomly chosen remote node,
         * otherwise ({@code null} if given service is not deployed on any node.
         */
        private ClusterNode randomNodeForService(String name) {
            if (hasLocNode && ctx.service().service(name) != null)
                return ctx.discovery().localNode();

            Map<UUID, Integer> snapshot = serviceTopology(name);

            if (snapshot == null || snapshot.isEmpty())
                return null;

            // Optimization for cluster singletons.
            if (snapshot.size() == 1) {
                UUID nodeId = snapshot.keySet().iterator().next();

                return prj.node(nodeId);
            }

            Collection<ClusterNode> nodes = prj.nodes();

            // Optimization for 1 node in projection.
            if (nodes.size() == 1) {
                ClusterNode n = nodes.iterator().next();

                return snapshot.containsKey(n.id()) ? n : null;
            }

            // Optimization if projection is the whole grid.
            if (prj.predicate() == F.<ClusterNode>alwaysTrue()) {
                int idx = ThreadLocalRandom8.current().nextInt(snapshot.size());

                int i = 0;

                // Get random node.
                for (Map.Entry<UUID, Integer> e : snapshot.entrySet()) {
                    if (i++ >= idx) {
                        if (e.getValue() > 0)
                            return ctx.discovery().node(e.getKey());
                    }
                }

                i = 0;

                // Circle back.
                for (Map.Entry<UUID, Integer> e : snapshot.entrySet()) {
                    if (e.getValue() > 0)
                        return ctx.discovery().node(e.getKey());

                    if (i++ == idx)
                        return null;
                }
            }
            else {
                List<ClusterNode> nodeList = new ArrayList<>(nodes.size());

                for (ClusterNode n : nodes) {
                    Integer cnt = snapshot.get(n.id());

                    if (cnt != null && cnt > 0)
                        nodeList.add(n);
                }

                if (nodeList.isEmpty())
                    return null;

                int idx = ThreadLocalRandom8.current().nextInt(nodeList.size());

                return nodeList.get(idx);
            }

            return null;
        }

        /**
         * @param name Service name.
         * @return Map of number of service instances per node ID.
         */
        private Map<UUID, Integer> serviceTopology(String name) {
            for (ServiceDescriptor desc : ctx.service().serviceDescriptors()) {
                if (desc.name().equals(name))
                    return desc.topologySnapshot();
            }

            return null;
        }
    }

    /**
     * Callable proxy class.
     */
    private static class ServiceProxyCallable implements IgniteCallable<Object>, Externalizable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Method name. */
        private String mtdName;

        /** Service name. */
        private String svcName;

        /** Argument types. */
        private Class[] argTypes;

        /** Args. */
        private Object[] args;

        /** Grid instance. */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ServiceProxyCallable() {
            // No-op.
        }

        /**
         * @param mtdName Service method to invoke.
         * @param svcName Service name.
         * @param argTypes Argument types.
         * @param args Arguments for invocation.
         */
        private ServiceProxyCallable(String mtdName, String svcName, Class[] argTypes, Object[] args) {
            this.mtdName = mtdName;
            this.svcName = svcName;
            this.argTypes = argTypes;
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            ServiceContextImpl svcCtx = ((IgniteKernal) ignite).context().service().serviceContext(svcName);

            if (svcCtx == null)
                throw new GridServiceNotFoundException(svcName);

            GridServiceMethodReflectKey key = new GridServiceMethodReflectKey(mtdName, argTypes);

            Method mtd = svcCtx.method(key);

            if (mtd == null)
                throw new GridServiceMethodNotFoundException(svcName, mtdName, argTypes);

            return mtd.invoke(svcCtx.service(), args);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, svcName);
            U.writeString(out, mtdName);
            U.writeArray(out, argTypes);
            U.writeArray(out, args);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            svcName = U.readString(in);
            mtdName = U.readString(in);
            argTypes = U.readClassArray(in);
            args = U.readArray(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ServiceProxyCallable.class, this);
        }
    }
}
