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

package org.apache.ignite.tests.p2p.classloadproblem;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toList;

/**
 * Common code used to simulate p2p class-loading problems.
 */
class P2PClassLoadingProblems {
    /***/
    static void triggerP2PClassLoadingProblem(Class<?> classLoadedViaP2P, Ignite ignite) {
        unregisterAllRemoteNodesFromP2PClassLoader(classLoadedViaP2P, ignite);
        triggerP2PClassLoad();
    }

    /***/
    private static void unregisterAllRemoteNodesFromP2PClassLoader(Class<?> classLoadedViaP2P, Ignite ignite) {
        ClassLoader classLoader = classLoadedViaP2P.getClassLoader();

        List<UUID> remoteNodeIds = ignite.cluster().forRemotes().nodes()
            .stream()
            .map(ClusterNode::id)
            .collect(toList());

        remoteNodeIds.forEach(remoteNodeId -> unregisterNodeOnClassLoader(classLoader, remoteNodeId));
    }

    /***/
    private static void unregisterNodeOnClassLoader(ClassLoader classLoader, UUID remoteNodeId) {
        Method unregisterMethod = findClassLoaderUnregisterMethod(classLoader);

        try {
            unregisterMethod.invoke(classLoader, remoteNodeId);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException(e);
        }
    }

    /***/
    @NotNull
    private static Method findClassLoaderUnregisterMethod(ClassLoader classLoader) {
        assert "GridDeploymentClassLoader".equals(classLoader.getClass().getSimpleName());

        Method unregisterMethod;
        try {
            unregisterMethod = classLoader.getClass().getDeclaredMethod("unregister", UUID.class);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException(e);
        }

        unregisterMethod.setAccessible(true);

        return unregisterMethod;
    }

    /***/
    private static void triggerP2PClassLoad() {
        @SuppressWarnings("unused")
        Object object = new SomeP2PClass();
    }

    /***/
    private P2PClassLoadingProblems() {
        // prevent instantiation
    }
}
