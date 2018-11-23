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

package org.apache.ignite.internal.processor.security.compute;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;

/**
 * Abstract compute security test.
 */
public abstract class AbstractComputeTaskSecurityTest extends AbstractResolveSecurityContextTest {
    /** */
    public void test() {
        checkSuccess(srvAllPerms, clntAllPerms);
        checkSuccess(srvAllPerms, srvReadOnlyPerm);
        checkSuccess(srvAllPerms, clntReadOnlyPerm);
        checkSuccess(clntAllPerms, srvAllPerms);
        checkSuccess(clntAllPerms, srvReadOnlyPerm);
        checkSuccess(clntAllPerms, clntReadOnlyPerm);

        checkFail(srvReadOnlyPerm, srvAllPerms);
        checkFail(srvReadOnlyPerm, clntAllPerms);
        checkFail(srvReadOnlyPerm, clntReadOnlyPerm);
        checkFail(clntReadOnlyPerm, srvAllPerms);
        checkFail(clntReadOnlyPerm, srvReadOnlyPerm);
        checkFail(clntReadOnlyPerm, clntAllPerms);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    protected abstract void checkSuccess(IgniteEx initiator, IgniteEx remote);

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    protected abstract void checkFail(IgniteEx initiator, IgniteEx remote);

    /**
     * Tri-consumer.
     *
     * @param <A> First parameter type.
     * @param <B> Second parameter type.
     * @param <C> Third parameter type.
     */
    @FunctionalInterface
    protected interface TriConsumer<A, B, C> {
        /**
         * Performs this operation on the given arguments.
         *
         * @param a First parameter.
         * @param b Second parameter.
         * @param c Third parameter.
         */
        void accept(A a, B b, C c);
    }
}