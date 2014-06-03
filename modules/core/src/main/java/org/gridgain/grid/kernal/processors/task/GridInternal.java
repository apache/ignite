/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.task;

import java.lang.annotation.*;

/**
 * Indicates that annotated task should always be loaded with local deployment,
 * ignoring grid source node configuration. Also jobs within such a task will
 * always be executed in the management thread pool on remote nodes and won't
 * generate job events.
 *
 * This annotation intended for internal use only.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridInternal {
    // No-op.
}
