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

package org.apache.ignite.ml.math.distributed;

import java.io.Serializable;

/**
 * Utility mapper that can be used to map arbitrary values types to and from double.
 */
public interface ValueMapper<V> extends Serializable {
    /**
     * @param v Value to map from double.
     * @return Mapped value.
     */
    public V fromDouble(double v);

    /**
     * @param v Value to map to double.
     * @return Mapped value.
     */
    public double toDouble(V v);
}
