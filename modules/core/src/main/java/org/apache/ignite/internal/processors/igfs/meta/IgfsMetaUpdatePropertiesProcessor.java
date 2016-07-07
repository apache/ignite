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

package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Update properties processor.
 */
public class IgfsMetaUpdatePropertiesProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Properties to be updated. */
    private Map<String, String> props;

    /**
     * Constructor.
     */
    public IgfsMetaUpdatePropertiesProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param props Properties.
     */
    public IgfsMetaUpdatePropertiesProcessor(Map<String, String> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        Map<String, String> tmp = oldInfo.properties();

        tmp = tmp == null ? new GridLeanMap<String, String>(props.size()) : new GridLeanMap<>(tmp);

        for (Map.Entry<String, String> e : props.entrySet()) {
            if (e.getValue() == null)
                // Remove properties with 'null' values.
                tmp.remove(e.getKey());
            else
                // Add/overwrite property.
                tmp.put(e.getKey(), e.getValue());
        }

        IgfsEntryInfo newInfo = oldInfo.properties(tmp);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        IgfsUtils.writeProperties(out, props);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        props = IgfsUtils.readProperties(in);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        IgfsUtils.writeProperties(out, props);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        props = IgfsUtils.readProperties(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaUpdatePropertiesProcessor.class, this);
    }
}
