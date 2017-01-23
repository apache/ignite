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

import org.apache.ignite.cluster.*;
import java.io.*;
import java.util.*;

/**
 * Pluggable provider for matrices and vectors based on their flavor.
 */
public interface MathProvider extends Externalizable {
    /**
     * Creates new matrix with given parameters.
     *
     * @param flavor Matrix flavor.
     * @param args Initialization parameters for the new matrix.
     * @param grp Optional cluster group (can be {@code null}).
     * @return Newly created matrix or an empty option in case given flavor is not supported.
     * @throws UnsupportedOperationException Thrown when given {@code flavor} or cluster group are not compatible.
     */
    Optional<Matrix> matrix(String flavor, Map<String, Object> args, ClusterGroup grp);

    /**
     * Creates new vector with given parameters.
     *
     * @param flavor Vector flavor.
     * @param args Initialization parameters for the new vector.
     * @param grp Optional cluster group (can be {@code null}).
     * @return Newly created vector or an empty option in case given flavor is not supported.
     * @throws UnsupportedOperationException Thrown when given {@code flavor} or cluster group are not compatible.
     */
    Optional<Vector> vector(String flavor, Map<String, Object> args, ClusterGroup grp);
}
