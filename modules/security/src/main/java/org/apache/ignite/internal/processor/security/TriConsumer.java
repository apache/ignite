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

package org.apache.ignite.internal.processor.security;

/**
 * Tri-consumer.
 *
 * @param <A> First parameter type.
 * @param <B> Second parameter type.
 * @param <C> Third parameter type.
 */
@FunctionalInterface
public interface TriConsumer<A, B, C> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param a First parameter.
     * @param b Second parameter.
     * @param c Third parameter.
     */
    public void accept(A a, B b, C c);
}
