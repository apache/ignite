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

package org.apache.ignite.compute.gridify;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
 * then Ignite searches for parameters with {@link GridifyInput} annotation.
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