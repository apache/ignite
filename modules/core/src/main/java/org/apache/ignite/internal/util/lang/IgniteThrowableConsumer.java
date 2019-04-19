/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * Represents an operation that accepts a single input argument and returns no result. Unlike most other functional
 * interfaces, {@code IgniteThrowableConsumer} is expected to operate via side-effects.
 * Also it is able to throw {@link IgniteCheckedException} unlike {@link java.util.function.Function}.
 *
 * @param <E> Type of closure parameter.
 */
public interface IgniteThrowableConsumer<E> extends Serializable {
    /**
     * Consumer body.
     *
     * @param e Consumer parameter.
     * @throws IgniteCheckedException If body execution was failed.
     */
    public void accept(E e) throws IgniteCheckedException;
}
