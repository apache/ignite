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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorCacheConfigurationCollectorTask}.
 */
public class VisorCacheConfigurationCollectorTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of cache names. */
    private Collection<String> cacheNames;

    /** Cache name regexp. */
    private String regex;

    /**
     * Default constructor.
     */
    public VisorCacheConfigurationCollectorTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames Collection of cache names.
     */
    public VisorCacheConfigurationCollectorTaskArg(Collection<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /**
     * @param regex Cache name regexp.
     */
    public VisorCacheConfigurationCollectorTaskArg(String regex) {
        // Checks, that regex is correct.
        Pattern.compile(regex);

        this.regex = regex;
    }

    /**
     * @return Collection of cache deployment IDs.
     */
    public Collection<String> getCacheNames() {
        return cacheNames;
    }

    /**
     * @return Cache name regexp.
     */
    public String getRegex() {
        return regex;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cacheNames);
        U.writeString(out, regex);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheNames = U.readCollection(in);

        if (protoVer > V1)
            regex = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfigurationCollectorTaskArg.class, this);
    }
}
