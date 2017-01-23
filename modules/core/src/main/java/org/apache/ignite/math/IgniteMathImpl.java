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

package org.apache.ignite.math;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import java.io.*;
import java.util.*;

/**
 * {@link IgniteMath} implementation.
 */
public class IgniteMathImpl extends AsyncSupportAdapter<IgniteMathImpl> implements IgniteMath, Externalizable {
    private GridKernalContext ctx;

    /**
     * Required by Externalizable interface.
     */
    public IgniteMathImpl() {
        // No-op.
    }

    /**
     * Creates new implementation.
     *
     * @param ctx Kernal context.
     * @param async Initial async mode.
     */
    public IgniteMathImpl(GridKernalContext ctx, boolean async) {
        super(async);

        this.ctx = ctx;
    }

    @Override
    public Matrix matrix(String flavor, Map<String, Object> args) {
        return null; // TODO
    }

    @Override
    public Matrix matrix(String flavor, Map<String, Object> args, ClusterGroup grp) {
        return null; // TODO
    }

    @Override
    public Vector vector(String flavor, Map<String, Object> args) {
        return null; // TODO
    }

    @Override
    public Vector vector(String flavor, Map<String, Object> args, ClusterGroup grp) {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
