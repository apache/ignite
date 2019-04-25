/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.jetbrains.annotations.Nullable;

/**
 * This class defines own object output stream.
 */
class JdkMarshallerObjectOutputStream extends ObjectOutputStream {
    /**
     * @param out Output stream.
     * @throws IOException Thrown in case of any I/O errors.
     */
    JdkMarshallerObjectOutputStream(OutputStream out) throws IOException {
        super(out);

        enableReplaceObject(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object replaceObject(Object o) throws IOException {
        return o == null || MarshallerExclusions.isExcluded(o.getClass()) ? null :
            o.getClass().equals(Object.class) ? new JdkMarshallerDummySerializable() : super.replaceObject(o);
    }
}
