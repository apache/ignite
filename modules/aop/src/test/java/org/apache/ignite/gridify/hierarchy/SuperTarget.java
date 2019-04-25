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

package org.apache.ignite.gridify.hierarchy;

import org.apache.ignite.compute.gridify.Gridify;

/**
 * Target base class.
 */
public abstract class SuperTarget {
    /**
     * @return Always returns "SuperTarget.methodA()".
     */
    @Gridify(igniteInstanceName = "GridifyHierarchyTest")
    protected String methodA() {
        System.out.println(">>> Called SuperTarget.methodA()");

        return "SuperTarget.methodA()";
    }

    /**
     * @return "SuperTarget.methodC()" string.
     */
    protected String methodB() {
        return methodC();
    }

    /**
     * @return "SuperTarget.methodC()" string.
     */
    @Gridify(igniteInstanceName = "GridifyHierarchyTest")
    private String methodC() {
        System.out.println(">>> Called SuperTarget.methodC()");

        return "SuperTarget.methodC()";
    }

}