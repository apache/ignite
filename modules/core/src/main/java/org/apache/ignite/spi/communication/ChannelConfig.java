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

package org.apache.ignite.spi.communication;

/**
 *
 */
public interface ChannelConfig {
    /**
     * Gets the channel's mode.
     */
    public boolean blocking();

    /**
     * @param blocking The channel's mode to set.
     * @return The config instance.
     */
    public ChannelConfig blocking(boolean blocking);

    /**
     * Gets the timeout option option.
     */
    public int timeout();

    /**
     * @param millis The timeout in milliseconds.
     * @return The config instance.
     */
    public ChannelConfig timeout(int millis);
}
