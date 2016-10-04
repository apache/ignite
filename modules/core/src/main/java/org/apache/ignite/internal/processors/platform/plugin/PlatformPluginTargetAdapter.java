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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.plugin.PlatformPluginTarget;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts user-defined IgnitePlatformPluginTarget to internal PlatformAbstractTarget.
 */
public class PlatformPluginTargetAdapter extends PlatformAbstractTarget {
    /** */
    private final PlatformPluginTarget target;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param target User-defined target.
     */
    public PlatformPluginTargetAdapter(PlatformContext platformCtx, PlatformPluginTarget target) {
        super(platformCtx);

        assert target != null;

        this.target = target;
    }

    /** {@inheritDoc} */
    @Override protected Object processInObjectStreamOutObjectStream(int type, @Nullable Object arg,
        BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {

        PlatformPluginTarget res = target.invokeOperation(type, reader, writer, arg);

        return res == null ? null : new PlatformPluginTargetAdapter(platformCtx, res);
    }
}
