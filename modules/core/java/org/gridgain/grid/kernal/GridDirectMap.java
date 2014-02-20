// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import java.lang.annotation.*;

/**
 * Annotates map fields.
 *
 * @author @java.author
 * @version @java.version
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface GridDirectMap {
    /**
     * @return Key type.
     */
    Class<?> keyType();

    /**
     * @return Value type.
     */
    Class<?> valueType();
}
