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

package org.apache.ignite.tests.p2p.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 */
public class ExternalCallable implements IgniteCallable {
    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** */
    private int param;

    /**
     */
    public ExternalCallable() {
        // No-op.
    }

    /**
     * @param param Param.
     */
    public ExternalCallable(int param) {
        this.param = param;
    }

    /** {@inheritDoc} */
    @Override public Object call() {
        System.err.println("!!!!! I am job " + param + " on " + ignite.name());

        return  42;
    }
}


