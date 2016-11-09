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

package org.apache.ignite.internal.processors.igfs.client;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS client summary callable.
 */
public class IgfsClientSummaryCallable extends IgfsClientAbstractCallable<IgfsPathSummary> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public IgfsClientSummaryCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param path Path.
     */
    public IgfsClientSummaryCallable(@Nullable String igfsName, IgfsPath path) {
        super(igfsName, path);
    }

    /** {@inheritDoc} */
    @Override protected IgfsPathSummary call0(IgfsContext ctx) throws Exception {
        return ctx.igfs().summary(path);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientSummaryCallable.class, this);
    }
}
