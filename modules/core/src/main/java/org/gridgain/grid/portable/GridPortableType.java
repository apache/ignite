/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import java.lang.annotation.*;

/**
 * Portable type annotation. Allows to provide
 * custom ID mapper and/or serializer.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GridPortableType {
    /**
     * Gets ID mapper class.
     *
     * @return ID mapper class.
     */
    public Class<? extends GridPortableIdMapper> idMapper();

    /**
     * Gets serializer class.
     *
     * @return Serializer class.
     */
    public Class<? extends GridPortableSerializer> serializer();
}
