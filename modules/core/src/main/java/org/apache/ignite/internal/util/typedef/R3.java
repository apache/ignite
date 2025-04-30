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

package org.apache.ignite.internal.util.typedef;

import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.IgniteReducer3;

/**
 * Defines {@code alias} for {@link org.apache.ignite.internal.util.lang.IgniteReducer3} by extending it.
 * Since Java doesn't provide type aliases (like Scala, for example) we resort to these types of measures.
 * This is intended to provide for more concise code in cases when readability won't be sacrificed.
 * For more information see {@link org.apache.ignite.internal.util.lang.IgniteReducer3}.
 * @param <E1> Type of the free variable, i.e. the element the closure is called or closed on.
 * @param <R> Type of the closure's return value.
 * @see GridFunc
 * @see org.apache.ignite.internal.util.lang.IgniteReducer3
 */
public interface R3<E1, E2, E3, R> extends IgniteReducer3<E1, E2, E3, R> { /* No-op. */ }
