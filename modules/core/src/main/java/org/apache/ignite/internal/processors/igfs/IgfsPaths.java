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

package org.apache.ignite.internal.processors.igfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Description of path modes.
 */
public class IgfsPaths implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] payloadBytes;

    /** Default IGFS mode. */
    private IgfsMode dfltMode;

    /** Path modes. */
    private ArrayList<T2<IgfsPath, IgfsMode>> pathModes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsPaths() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param payload Payload.
     * @param dfltMode Default IGFS mode.
     * @param pathModes Path modes.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsPaths(Object payload, IgfsMode dfltMode, @Nullable ArrayList<T2<IgfsPath, IgfsMode>> pathModes)
        throws IgniteCheckedException {
        this.dfltMode = dfltMode;
        this.pathModes = pathModes;

        if (payload == null)
            payloadBytes = null;
        else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            new JdkMarshaller().marshal(payload, out);

            payloadBytes = out.toByteArray();
        }
    }

    /**
     * @return Default IGFS mode.
     */
    public IgfsMode defaultMode() {
        return dfltMode;
    }

    /**
     * @return Path modes.
     */
    @Nullable public ArrayList<T2<IgfsPath, IgfsMode>> pathModes() {
        return pathModes;
    }

    /**
     * @return Payload.
     *
     * @throws IgniteCheckedException If failed to deserialize the payload.
     */
    @Nullable public Object getPayload(ClassLoader clsLdr) throws IgniteCheckedException {
        if (payloadBytes == null)
            return null;
        else {
            ByteArrayInputStream in = new ByteArrayInputStream(payloadBytes);

            return new JdkMarshaller().unmarshal(in, clsLdr);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, payloadBytes);

        U.writeEnum(out, dfltMode);

        if (pathModes != null) {
            out.writeBoolean(true);
            out.writeInt(pathModes.size());

            for (T2<IgfsPath, IgfsMode> pathMode : pathModes) {
                assert pathMode.getKey() != null;

                pathMode.getKey().writeExternal(out);

                U.writeEnum(out, pathMode.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        payloadBytes = U.readByteArray(in);

        dfltMode = IgfsMode.fromOrdinal(in.readByte());

        if (in.readBoolean()) {
            int size = in.readInt();

            pathModes = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                IgfsPath path = new IgfsPath();

                path.readExternal(in);

                pathModes.add(new T2<>(path, IgfsMode.fromOrdinal(in.readByte())));
            }
        }
    }
}