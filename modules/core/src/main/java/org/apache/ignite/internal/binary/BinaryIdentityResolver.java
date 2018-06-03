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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;
import org.jetbrains.annotations.Nullable;

/**
 * Interface to compute hash codes for new binary objects and compare them for equality.
 */
public interface BinaryIdentityResolver {
    /**
     * Compute hash code for binary object.
     *
     * @param obj Binary object.
     * @return Hash code value.
     */
    public int hashCode(BinaryObject obj);

    /**
     * Compare two binary objects for equality.
     *
     * @param o1 First object.
     * @param o2 Second object.
     * @return {@code True} if both objects are equal.
     */
    public boolean equals(@Nullable BinaryObject o1, @Nullable BinaryObject o2);
}
