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
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Defines {@code alias} for {@link org.apache.ignite.lang.IgniteBiPredicate} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.apache.ignite.lang.IgniteBiPredicate}.
 * @param <T1> Type of the first free variable, i.e. the element the closure is called on.
 * @param <T2> Type of the second free variable, i.e. the element the closure is called on.
 * @see GridFunc
 * @see org.apache.ignite.lang.IgniteBiPredicate
 */
public interface P2<T1, T2> extends IgniteBiPredicate<T1, T2> { /* No-op. */ }