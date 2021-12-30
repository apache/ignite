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

package org.apache.ignite.internal.processors.service;

import org.jetbrains.annotations.Nullable;

/**
 * Holder of the context of the service call.
 */
public class ServiceCallContextHolder {
    /** Service call context of the current thread. */
    private static final ThreadLocal<ServiceCallContextImpl> locCallCtx = new ThreadLocal<>();

    /**
     * @return Service call context of the current thread.
     */
    @Nullable public static ServiceCallContextImpl current() {
        return locCallCtx.get();
    }

    /**
     * @param callCtx Service call context of the current thread.
     */
    static void current(@Nullable ServiceCallContextImpl callCtx) {
        if (callCtx != null)
            locCallCtx.set(callCtx);
        else
            locCallCtx.remove();
    }
}
