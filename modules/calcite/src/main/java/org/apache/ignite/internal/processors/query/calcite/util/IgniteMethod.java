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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.lang.reflect.Method;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.DerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.FragmentMetadata;

/**
 * Contains methods used in metadata definitions.
 */
public enum IgniteMethod {
    DERIVED_DISTRIBUTIONS(DerivedDistribution.class, "deriveDistributions"),
    FRAGMENT_INFO(FragmentMetadata.class, "fragmentInfo");

    /** */
    private final Method method;

    /**
     * @param clazz Class where to lookup method.
     * @param methodName Method name.
     * @param argumentTypes Method parameters types.
     */
    IgniteMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

    /**
     * @return Method.
     */
    public Method method() {
        return method;
    }
}
