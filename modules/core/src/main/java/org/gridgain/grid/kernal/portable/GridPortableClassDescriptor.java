/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jdk8.backport.*;

/**
 * Portable class descriptor.
 */
public class GridPortableClassDescriptor {
    private static final ConcurrentHashMap8<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    public static GridPortableClassDescriptor get(Class<?> cls) {
        assert cls != null;

        GridPortableClassDescriptor desc = CACHE.get(cls);

        if (desc == null) {
            GridPortableClassDescriptor old = CACHE.putIfAbsent(cls, desc = new GridPortableClassDescriptor(cls));

            if (old != null)
                desc = old;
        }

        return desc;
    }

    public GridPortableClassDescriptor(Class<?> cls) {

    }

    public void write(GridPortableWriter writer) {

    }
}
