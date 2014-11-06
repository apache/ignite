/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import java.util.*;

/**
 * Method reflection key.
 */
class GridServiceMethodReflectKey {
    /** Method name. */
    private final String mtdName;

    /** Argument types. */
    private final Class<?>[] argTypes;

    /** Hash code. */
    private int hash;

    /**
     * @param mtdName Method name.
     * @param args Arguments.
     */
    GridServiceMethodReflectKey(String mtdName, Object[] args) {
        assert mtdName != null;

        this.mtdName = mtdName;

        argTypes = new Class<?>[args == null ? 0 : args.length];

        for (int i = 0; args != null && i < args.length; i++)
            argTypes[i] = args[i].getClass();
    }

    /**
     * @return Method name.
     */
    String methodName() {
        return mtdName;
    }

    /**
     * @return Arg types.
     */
    Class<?>[] argTypes() {
        return argTypes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridServiceMethodReflectKey key = (GridServiceMethodReflectKey)o;

        return mtdName.equals(key.mtdName) && Arrays.equals(argTypes, key.argTypes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        if (hash != 0)
            return hash;

        return hash = 31 * mtdName.hashCode() + Arrays.hashCode(argTypes);
    }
}
