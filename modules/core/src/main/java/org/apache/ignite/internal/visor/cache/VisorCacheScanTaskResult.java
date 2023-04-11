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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of running {@link VisorCacheScanTask}.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class VisorCacheScanTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column titles. */
    private List<String> titles;

    /** Cache entries. */
    private List<List<?>> entries;

    /**
     *
     */
    public VisorCacheScanTaskResult() {
    }

    /**
     * @param entries Cache entries.
     */
    public VisorCacheScanTaskResult(List<String> titles, List<List<?>> entries) {
        this.titles = titles;
        this.entries = entries;
    }

    /**
     * @return Column titles.
     */
    public List<String> titles() {
        return titles;
    }

    /**
     * @return Cache entries.
     */
    public List<List<?>> entries() {
        return entries;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, titles);
        U.writeCollection(out, entries);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        titles = U.readList(in);
        entries = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheScanTaskResult.class, this);
    }
}
