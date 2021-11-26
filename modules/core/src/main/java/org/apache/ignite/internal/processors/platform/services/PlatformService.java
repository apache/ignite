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

package org.apache.ignite.internal.processors.platform.services;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.services.Service;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for all platform services.
 */
public interface PlatformService extends Service {
    /**
     * Invokes native service method.
     *
     * @param mthdName Method name.
     * @param srvKeepBinary Server keep binary flag.
     * @param deserializeResult If {@code true}, call service in cross-platform compatible manner.
     * @param args Arguments.
     * @param callAttrs Service call context attributes.
     * @return Resulting data.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    public Object invokeMethod(
        String mthdName,
        boolean srvKeepBinary,
        boolean deserializeResult,
        @Nullable Object[] args,
        @Nullable Map<String, Object> callAttrs
    ) throws IgniteCheckedException;

    /**
     * Gets native pointer.
     *
     * @return Native pointer.
     */
    public long pointer();
}
