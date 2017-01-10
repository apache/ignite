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

package org.apache.ignite.internal.processors.platform.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;

/**
 * Proxy that implements PlatformTarget.
 */
public class PlatformContinuousQueryProxy extends PlatformAbstractTarget  {
    private final PlatformContinuousQuery qry;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformContinuousQueryProxy(PlatformContext platformCtx, PlatformContinuousQuery qry) {
        super(platformCtx);

        assert qry != null;

        this.qry = qry;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        return qry.getInitialQueryCursor();
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        qry.close();

        return 0;
    }
}
