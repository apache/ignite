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

package org.apache.ignite.internal.processors.rest.client.message;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.portables.*;

import java.util.*;

/**
 * Metadata put request.
 */
public class GridClientPutMetaDataRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Collection<GridClientPortableMetaData> meta;

    /**
     * @return Type IDs.
     */
    public Collection<GridClientPortableMetaData> metaData() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        super.writePortable(writer);

        PortableRawWriter raw = writer.rawWriter();

        raw.writeCollection(meta);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        super.readPortable(reader);

        PortableRawReader raw = reader.rawReader();

        meta = raw.readCollection();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPutMetaDataRequest.class, this);
    }
}
