/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.optimized;

import java.util.*;

/**
 * Optional interface which helps make serialization even faster by removing internal
 * look-ups for classes.
 * <p>
 * All implementation must have the following:
 * <ul>
 * <li>
 *     Must have static filed (private or public) declared of type {@link Object}
 *     with name {@code GG_CLASS_ID}. GridGain will reflectively initialize this field with
 *     proper class ID during system startup.
 * </li>
 * <li>
 *     Must return the value of {@code GG_CLASS_ID} field from {@link #ggClassId} method.
 * </li>
 * </ul>
 * Here is a sample implementation:
 * <pre name="code" class="java">
 * // For better performance consider implementing java.io.Externalizable interface.
 * class ExampleMarshallable implements GridOptimizedMarshallable, Serializable {
 *     // Class ID field required by 'GridOptimizedMarshallable'.
 *     private static Object GG_CLASS_ID;
 *
 *     ...
 *
 *     &#64; public Object ggClassId() {
 *         return GG_CLASS_ID;
 *     }
 * }
 * </pre>
 * <p>
 * Note that for better performance you should also specify list of classes you
 * plan to serialize via {@link GridOptimizedMarshaller#setClassNames(List)} method.
 */
public interface GridOptimizedMarshallable {
    /** */
    public static final String CLS_ID_FIELD_NAME = "GG_CLASS_ID";

    /**
     * Implementation of this method should simply return value of {@code GG_CLASS_ID} field.
     *
     * @return Class ID for optimized marshalling.
     */
    public Object ggClassId();
}
