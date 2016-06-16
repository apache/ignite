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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * IGFS client mkdirs callable.
 */
public class IgfsClientMkdirsCallable extends IgfsClientAbstractCallable<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Properties. */
    private Map<String, String> props;

    /**
     * Default constructor.
     */
    public IgfsClientMkdirsCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param path Path.
     * @param props Properties.
     */
    public IgfsClientMkdirsCallable(@Nullable String igfsName, IgfsPath path, @Nullable Map<String, String> props) {
        super(igfsName, path);

        this.props = props;
    }

    /** {@inheritDoc} */
    @Override protected Void call0(IgfsContext ctx) throws Exception {
        ctx.igfs().mkdirs(path, props);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary0(BinaryRawWriter writer) throws BinaryObjectException {
        IgfsUtils.writeProperties(writer, props);
    }

    /** {@inheritDoc} */
    @Override public void readBinary0(BinaryRawReader reader) throws BinaryObjectException {
        props = IgfsUtils.readProperties(reader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientMkdirsCallable.class, this);
    }
}
