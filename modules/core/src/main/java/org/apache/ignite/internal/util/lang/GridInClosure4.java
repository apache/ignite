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

/**
 * @param <E1> Type of the first free variable.
 * @param <E2> Type of the second free variable.
 * @param <E3> Type of the third free variable.
 * @param <E4> Type of the Fourth free variable.
 */
public interface GridInClosure4<E1, E2, E3, E4> {
    /**
     * @param e1 First bound free variable.
     * @param e2 Second bound free variable.
     * @param e3 Third bound free variable.
     * @param e4 Fourth bound free variable.
     */
    public void apply(E1 e1, E2 e2, E3 e3, E4 e4);
}
