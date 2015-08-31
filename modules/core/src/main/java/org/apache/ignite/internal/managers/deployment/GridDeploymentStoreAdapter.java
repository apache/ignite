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

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for all store implementations.
 */
abstract class GridDeploymentStoreAdapter implements GridDeploymentStore {
    /** Logger. */
    protected final IgniteLogger log;

    /** Deployment SPI. */
    protected final DeploymentSpi spi;

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Deployment communication. */
    protected final GridDeploymentCommunication comm;

    /**
     * @param spi Underlying SPI.
     * @param ctx Grid kernal context.
     * @param comm Deployment communication.
     */
    GridDeploymentStoreAdapter(DeploymentSpi spi, GridKernalContext ctx, GridDeploymentCommunication comm) {
        assert spi != null;
        assert ctx != null;
        assert comm != null;

        this.spi = spi;
        this.ctx = ctx;
        this.comm = comm;

        log = ctx.config().getGridLogger().getLogger(getClass());
    }

    /**
     * @return Startup log message.
     */
    protected final String startInfo() {
        return "Deployment store started: " + this;
    }

    /**
     * @return Stop log message.
     */
    protected final String stopInfo() {
        return "Deployment store stopped: " + this;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Ignoring kernel started callback: " + this);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Ignoring explicit deploy [cls=" + cls + ", clsLdr=" + clsLdr + ']');

        return null;
    }

    /**
     * @param ldr Class loader.
     * @return User version.
     */
    protected final String userVersion(ClassLoader ldr) {
        return ctx.userVersion(ldr);
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if class is task class.
     */
    protected final boolean isTask(Class<?> cls) {
        return ComputeTask.class.isAssignableFrom(cls);
    }

    /**
     * Clears serialization caches to avoid PermGen memory leaks.
     * This method should be called on each undeployment.
     * <p>
     * For more information: http://www.szegedi.org/articles/memleak3.html.
     */
    protected final void clearSerializationCaches() {
        try {
            clearSerializationCache(Class.forName("java.io.ObjectInputStream$Caches"), "subclassAudits");
            clearSerializationCache(Class.forName("java.io.ObjectOutputStream$Caches"), "subclassAudits");
            clearSerializationCache(Class.forName("java.io.ObjectStreamClass$Caches"), "localDescs");
            clearSerializationCache(Class.forName("java.io.ObjectStreamClass$Caches"), "reflectors");
        }
        catch (ClassNotFoundException e) {
            if (log.isDebugEnabled())
                log.debug("Class not found: " + e.getMessage());
        }
        catch (NoSuchFieldException e) {
            if (log.isDebugEnabled())
                log.debug("Field not found: " + e.getMessage());
        }
        catch (IllegalAccessException e) {
            if (log.isDebugEnabled())
                log.debug("Field can't be accessed: " + e.getMessage());
        }
    }

    /**
     * @param cls Class name.
     * @param fieldName Field name.
     * @throws IllegalAccessException If field can't be accessed.
     * @throws NoSuchFieldException If class can't be found.
     */
    private void clearSerializationCache(Class cls, String fieldName)
        throws NoSuchFieldException, IllegalAccessException {
        Field f = cls.getDeclaredField(fieldName);

        f.setAccessible(true);

        ((Map)f.get(null)).clear();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentStoreAdapter.class, this);
    }
}