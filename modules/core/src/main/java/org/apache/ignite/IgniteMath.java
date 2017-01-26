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

package org.apache.ignite;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import java.util.*;

/**
 * Defines distributed core algebra support for dense and sparse data sets.
 *
 * List of matrix flavors supported by default:
 * <ul>
 * <li>{@code "dense.local.onheap"}</li>
 * </ul>
 *
 * List of vector flavors supported by default:
 * <ul>
 * <li>{@code "dense.local.onheap"}</li>
 * </ul>
 */
public interface IgniteMath extends IgniteAsyncSupport {
    /**
     * Creates new local matrix with given {@code flavor}.
     *
     * @param flavor Matrix flavor.
     * @param args Optional initialization parameters for the new matrix.
     * @return Newly created matrix.
     * @throws UnknownProviderException Thrown when no provider can be found for given {@code flavor}.
     * @throws UnsupportedOperationException Thrown when provider is found but matrix cannot be created
     *      based on the given arguments.
     */
    Matrix matrix(String flavor, Map<String, Object> args);

    /**
     * Creates new distributed matrix with given {@code flavor}.
     * 
     * @param flavor Matrix flavor.
     * @param args Optional initialization parameters for the new matrix.
     * @param grp Cluster group to be used.
     * @return Newly created matrix.
     * @throws UnknownProviderException Thrown when no provider can be found for given {@code flavor}.
     * @throws UnsupportedOperationException Thrown when provider is found but matrix cannot be created
     *      based on the given arguments.
     */
    Matrix matrix(String flavor, Map<String, Object> args, ClusterGroup grp);

    /**
     * Creates new local vector with given {@code flavor}.
     *
     * @param flavor Vector flavor.
     * @param args Optional initialization parameters for the new vector.
     * @return Newly created vector.
     * @throws UnknownProviderException Thrown when no provider can be found for given {@code flavor}.
     * @throws UnsupportedOperationException Thrown when provider is found but vector cannot be created
     *      based on the given arguments.
     */
    Vector vector(String flavor, Map<String, Object> args);

    /**
     * Creates new distributed vector with given {@code flavor}.
     *
     * @param flavor Vector flavor.
     * @param args Optional initialization parameters for the new vector.
     * @param grp Cluster group to be used.
     * @return Newly created vector.
     * @throws UnknownProviderException Thrown when no provider can be found for given {@code flavor}.
     * @throws UnsupportedOperationException Thrown if cluster group is not
     *      applicable for given vector's {@code flavor}.
     * @throws UnsupportedOperationException Thrown when provider is found but vector cannot be created
     *      based on the given arguments.
     */
    Vector vector(String flavor, Map<String, Object> args, ClusterGroup grp);
}
