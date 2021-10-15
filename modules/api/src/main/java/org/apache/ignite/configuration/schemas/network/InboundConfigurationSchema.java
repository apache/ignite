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

package org.apache.ignite.configuration.schemas.network;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Max;
import org.apache.ignite.configuration.validation.Min;

/**
 * Server socket configuration. See <a href="https://man7.org/linux/man-pages/man7/tcp.7.html">TCP docs</a> and
 * <a href="https://man7.org/linux/man-pages/man7/socket.7.html">socket docs</a>.
 */
@Config
public class InboundConfigurationSchema {
    /** Backlog value */
    @Min(0)
    @Value(hasDefault = true)
    public final int soBacklog = 128;

    /** Reuse address flag. */
    @Value(hasDefault = true)
    public final boolean soReuseAddr = true;

    /** Keep-alive flag. */
    @Value(hasDefault = true)
    public final boolean soKeepAlive = true;

    /** Socket close linger value. */
    @Min(0)
    @Max(0xFFFF)
    @Value(hasDefault = true)
    public final int soLinger = 0;

    /** TCP no delay flag. */
    @Value(hasDefault = true)
    public final boolean tcpNoDelay = true;
}
