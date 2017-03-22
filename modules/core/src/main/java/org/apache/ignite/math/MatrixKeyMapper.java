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

package org.apache.ignite.math;

import java.io.Serializable;

/**
 * TODO: add description.
 */
public interface MatrixKeyMapper<K> extends Serializable {
    /**
     * 
     * @param x
     * @param y
     * @return
     */
    public K apply(int x, int y);

    /**
     * Checks that a pair (x, y) exists for which method {@link #apply(int, int)} will return 'k'.
     *
     * @param k Key to check.
     */
    public boolean isValid(K k);
}
