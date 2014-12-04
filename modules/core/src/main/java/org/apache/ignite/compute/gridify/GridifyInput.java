/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute.gridify;

import java.lang.annotation.*;

/**
 * This annotation can be applied to method parameter for grid-enabled method. This annotation can be used
 * when using when using annotations {@link GridifySetToValue} and {@link GridifySetToSet}.
 * Note that annotations {@link GridifySetToValue} and {@link GridifySetToSet} can be applied
 * to methods with a different number of parameters of the following types
 * <ul>
 * <li>java.util.Collection</li>
 * <li>java.util.Iterator</li>
 * <li>java.util.Enumeration</li>
 * <li>java.lang.CharSequence</li>
 * <li>java array</li>
 * </ul>
 * If grid-enabled method contains several parameters with types described above
 * then GridGain searches for parameters with {@link GridifyInput} annotation.
 * <b>Only one</b> method parameter with {@link GridifyInput} annotation allowed.
 * @see GridifySetToValue
 * @see GridifySetToSet
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface GridifyInput {
    // No-op.
}
