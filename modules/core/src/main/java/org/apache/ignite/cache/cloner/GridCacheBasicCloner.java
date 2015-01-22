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

package org.apache.ignite.cache.cloner;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;

/**
 * Basic cache cloner based on utilization of {@link Cloneable} interface. If
 * a passed in object implements {@link Cloneable} then its implementation of
 * {@link Object#clone()} is used to get a copy. Otherwise, the object itself
 * will be returned without cloning.
 */
public class GridCacheBasicCloner implements GridCacheCloner {
    /** {@inheritDoc} */
    @Override public <T> T cloneValue(T val) throws IgniteCheckedException {
        return X.cloneObject(val, false, true);
    }
}
