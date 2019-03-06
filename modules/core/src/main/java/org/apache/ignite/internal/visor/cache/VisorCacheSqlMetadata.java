/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for cache SQL metadata.
 */
public class VisorCacheSqlMetadata extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String cacheName;

    /** */
    private List<String> types;

    /** */
    private Map<String, String> keyClasses;

    /** */
    private Map<String, String> valClasses;

    /** */
    private Map<String, Map<String, String>> fields;

    /** */
    private Map<String, List<VisorCacheSqlIndexMetadata>> indexes;

    /**
     * Default constructor.
     */
    public VisorCacheSqlMetadata() {
        // No-op.
    }

    /**
     * Create data transfer object.
     *
     * @param meta Cache SQL metadata.
     */
    public VisorCacheSqlMetadata(GridCacheSqlMetadata meta) {
        cacheName = meta.cacheName();
        types = toList(meta.types());
        keyClasses = meta.keyClasses();
        valClasses = meta.valClasses();
        fields = meta.fields();
        indexes = new HashMap<>();

        Map<String, Collection<GridCacheSqlIndexMetadata>> src = meta.indexes();

        if (src != null) {
            for (Map.Entry<String, Collection<GridCacheSqlIndexMetadata>> entry: src.entrySet()) {
                Collection<GridCacheSqlIndexMetadata> idxs = entry.getValue();

                List<VisorCacheSqlIndexMetadata> res = new ArrayList<>(idxs.size());

                for (GridCacheSqlIndexMetadata idx : idxs)
                    res.add(new VisorCacheSqlIndexMetadata(idx));

                indexes.put(entry.getKey(), res);
            }
        }
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Collection of available types.
     */
    public List<String> getTypes() {
        return types;
    }

    /**
     * @return Key classes.
     */
    public Map<String, String> getKeyClasses() {
        return keyClasses;
    }

    /**
     * @return Value classes.
     */
    public Map<String, String> getValueClasses() {
        return valClasses;
    }

    /**
     * @return Fields.
     */
    public Map<String, Map<String, String>> getFields() {
        return fields;
    }

    /**
     * @return Indexes.
     */
    public Map<String, List<VisorCacheSqlIndexMetadata>> getIndexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeCollection(out, types);
        IgfsUtils.writeStringMap(out, keyClasses);
        IgfsUtils.writeStringMap(out, valClasses);
        U.writeMap(out, fields);
        U.writeMap(out, indexes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        types = U.readList(in);
        keyClasses = IgfsUtils.readStringMap(in);
        valClasses = IgfsUtils.readStringMap(in);
        fields = U.readMap(in);
        indexes = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheSqlMetadata.class, this);
    }
}
