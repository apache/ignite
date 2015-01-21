/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * plan to serialize via {@link IgniteOptimizedMarshaller#setClassNames(List)} method.
 */
public interface IgniteOptimizedMarshallable {
    /** */
    public static final String CLS_ID_FIELD_NAME = "GG_CLASS_ID";

    /**
     * Implementation of this method should simply return value of {@code GG_CLASS_ID} field.
     *
     * @return Class ID for optimized marshalling.
     */
    public Object ggClassId();
}
