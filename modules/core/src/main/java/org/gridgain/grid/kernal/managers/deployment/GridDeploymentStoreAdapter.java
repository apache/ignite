/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.deployment;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Adapter for all store implementations.
 */
abstract class GridDeploymentStoreAdapter implements GridDeploymentStore {
    /** Logger. */
    protected final GridLogger log;

    /** Deployment SPI. */
    protected final GridDeploymentSpi spi;

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Deployment communication. */
    protected final GridDeploymentCommunication comm;

    /**
     * @param spi Underlying SPI.
     * @param ctx Grid kernal context.
     * @param comm Deployment communication.
     */
    GridDeploymentStoreAdapter(GridDeploymentSpi spi, GridKernalContext ctx, GridDeploymentCommunication comm) {
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
    @Override public void onKernalStart() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Ignoring kernel started callback: " + this);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws GridException {
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
