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
 * Annotation for enabling task session attributes and checkpoints for tasks.
 * <p>
 * Use this annotation when planning to use checkpoints or task session attributes API to
 * distribute session attributes between jobs.
 * <p>
 * By default attributes and checkpoints are disabled for performance reasons.
 * @see ComputeTaskSession
 * @see ComputeTaskSession#setAttribute(Object, Object)
 * @see ComputeTaskSession#setAttributes(Map)
 * @see ComputeTaskSession#addAttributeListener(ComputeTaskSessionAttributeListener, boolean)
 * @see ComputeTaskSession#saveCheckpoint(String, Object)
 * @see ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long)
 * @see ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long, boolean)
 * @see ComputeTaskSession#loadCheckpoint(String)
 * @see ComputeTaskSession#removeCheckpoint(String)
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskSessionFullSupport {
    // No-op.
}
