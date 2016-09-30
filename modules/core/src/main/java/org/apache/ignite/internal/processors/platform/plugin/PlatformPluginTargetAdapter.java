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

package org.apache.ignite.internal.processors.platform.plugin;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.plugin.IgnitePlatformPluginTarget;

/**
 * Adapts user-defined IgnitePlatformPluginTarget to internal PlatformAbstractTarget.
 */
public class PlatformPluginTargetAdapter extends PlatformAbstractTarget implements IgnitePlatformPluginTarget {
    /** */
    private final IgnitePlatformPluginTarget target;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param pluginTarget
     */
    public PlatformPluginTargetAdapter(PlatformContext platformCtx, IgnitePlatformPluginTarget pluginTarget) {
        super(platformCtx);

        assert pluginTarget != null;

        this.target = pluginTarget;
    }

    /** {@inheritDoc} */
    @Override public void invokeOperation(int opCode, BinaryRawReader reader, BinaryRawWriter writer)
        throws IgniteException {

        // TODO: Delegate to super

    }
}
