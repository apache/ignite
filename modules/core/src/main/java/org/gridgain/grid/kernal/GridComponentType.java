// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;

import java.lang.reflect.*;

/**
 * Component type.
 */
public enum GridComponentType {
    /** GGFS component. */
    COMP_GGFS(
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsNoopProcessor",
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsProcessor"
    ),

    /** Hadoop. */
    COMP_HADOOP(
        "org.gridgain.grid.kernal.processors.hadoop.GridHadoopNoopProcessor",
        "org.gridgain.grid.kernal.processors.hadoop.GridHadoopProcessor"
    );

    /** No-op class name. */
    private final String noOpClsName;

    /** Class name. */
    private final String clsName;

    /**
     * Constructor.
     *
     * @param clsName Class name.
     */
    GridComponentType(String noOpClsName, String clsName) {
        this.noOpClsName = noOpClsName;
        this.clsName = clsName;
    }

    public String className() {
        return clsName;
    }

    /**
     * Create component.
     *
     * @param ctx Kernal context.
     * @param noOp No-op flag.
     * @return Created component.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <T extends GridComponent> T create(GridKernalContext ctx, boolean noOp) throws GridException {
        return create0(ctx, noOp ? noOpClsName : clsName);
    }

    /**
     * Create component instance.
     *
     * @param ctx Kernal context.
     * @param clsName Component class name.
     * @return Component instance.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private <T extends GridComponent> T create0(GridKernalContext ctx, String clsName) throws GridException {
        try {
            Class<?> cls = Class.forName(clsName);

            Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

            return (T)ctor.newInstance(ctx);
        }
        catch (ClassNotFoundException e) {
            throw new GridException("Failed to create GridGain component because it's class is not found " +
                "(is it in classpath?) [component=" + this + ", class=" + clsName + ']', e);
        }
        catch (NoSuchMethodException e) {
            throw new GridException("Failed to create GridGain component because it's class doesn't have " +
                "required constructor (is class version correct?) [component=" + this + ", class=" +
                clsName + ']', e);
        }
        catch (ReflectiveOperationException e) {
            throw new GridException("Failed to instantiate GridGain component [component=" + this + ", class=" +
                clsName + ']', e);
        }
    }
}
