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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.storage.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class VectorView extends AbstractVector {
    // Parent.
    private Vector parent;

    // View offset.
    private int off;

    // View length.
    private int len;

    /**
     *
     * @param parent
     * @param off
     * @param len
     */
    public VectorView(Vector parent, int off, int len) {
        super(new VectorDelegateStorage(parent.getStorage(), off, len));

        this.parent = parent;
        this.off = off;
        this.len = len;
    }

    @Override
    public Vector copy() {
        return new VectorView(parent, off, len);
    }

    @Override
    public Vector like(int crd) {
        return parent.like(crd);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(parent);
        out.writeInt(off);
        out.writeInt(len);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parent = (Vector)in.readObject();
        off = in.readInt();
        len = in.readInt();
    }
}
