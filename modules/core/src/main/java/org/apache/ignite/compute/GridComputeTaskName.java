/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import java.lang.annotation.*;

/**
 * This annotation allows to assign optional name to grid task. If attached to
 * {@link GridComputeTask} implementation GridGain will take this name as a task name
 * instead of default which is grid task's class FQN.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridComputeTaskName {
    /**
     * Optional task name.
     */
    @SuppressWarnings({"JavaDoc"}) String value();
}
