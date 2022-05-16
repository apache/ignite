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

package org.apache.ignite.internal.processors.platform.compute;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Runnable implementation that delegates to native platform.
 */
public class PlatformRunnable extends PlatformAbstractFunc implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param func Platform func.
     * @param ptr Handle for local execution.
     */
    public PlatformRunnable(Object func, long ptr) {
        super(func, ptr);
    }

    /** <inheritdoc /> */
    @Override protected void platformCallback(PlatformCallbackGateway gate, long memPtr) {
        gate.computeActionExecute(memPtr);
    }

    /** <inheritdoc /> */
    @Override public void run() {
        try {
            invoke();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}
