/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import java.util.*;

/**
 * This interface defines listener for task session attributes.
 */
public interface GridComputeTaskSessionAttributeListener extends EventListener {
    /**
     * Called on attribute change (set or update).
     *
     * @param key Attribute key.
     * @param val Attribute value.
     */
    public void onAttributeSet(Object key, Object val);
}
