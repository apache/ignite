/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lang;

import java.lang.annotation.*;

/**
 * Annotation to indicate that method can be executed asynchronously if async mode is enabled.
 * To enable async mode, invoke {@link IgniteAsyncSupport#enableAsync()} method on the API.
 * The future for the async method can be retrieved via {@link IgniteAsyncSupport#future()} method
 * right after the execution of an asynchronous method.
 *
 * TODO coding example.
 *
 * @see IgniteAsyncSupport
 */
@Documented
@Target(ElementType.METHOD)
public @interface IgniteAsyncSupported {

}
