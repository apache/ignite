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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Represents a function that accepts two arguments and produces a result.
 *
 * @param <E> Type of first closure parameter.
 * @param <U> Type of second closure parameter.
 * @param <R> Type of result value.
 */
public interface IgniteThrowableBiFunction<E, U, R> extends Serializable {
    /**
     * Consumer body.
     *
     * @param e Consumer parameter.
     * @throws IgniteCheckedException if body execution was failed.
     */
    public R accept(E e, U r) throws IgniteCheckedException;
}
