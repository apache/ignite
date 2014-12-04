/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import java.lang.annotation.*;
import java.util.*;

/**
 * Annotation for asynchronous task mapping.
 * <p>
 * This annotation enables map step ({@link ComputeTask#map(List, Object)}) for task
 * to be performed asynchronously when attached to {@link ComputeTask} class being executed.
 * <p>
 * Use this annotation when tasks spawns large amount of jobs or map step takes a long time
 * and it is better to perform it in GridGain system thread.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskMapAsync {
    // No-op.
}
