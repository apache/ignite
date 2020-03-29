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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.plugin.Extension;

/**
 * Platform extension.
 */
public interface PlatformPluginExtension extends Extension {
    /**
     * Get extension ID. Must be unique among all extensions.
     *
     * @return Extension ID.
     */
    int id();

    /**
     * Creates the PlatformTarget for this extension.
     *
     * @return PlatformTarget for this extension.
     */
    PlatformTarget createTarget();
}
