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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.services.ServiceConfiguration;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for configuration of service data structures.
 */
public class VisorServiceConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private String name;

    /** Service instance. */
    private String svc;

    /** Total count. */
    private int totalCnt;

    /** Max per-node count. */
    private int maxPerNodeCnt;

    /** Cache name. */
    private String cacheName;

    /** Affinity key. */
    private String affKey;

    /** Node filter. */
    private String nodeFilter;

    /**
     * Construct data transfer object for service configurations properties.
     *
     * @param cfgs Service configurations.
     * @return Service configurations properties.
     */
    public static List<VisorServiceConfiguration> list(ServiceConfiguration[] cfgs) {
        List<VisorServiceConfiguration> res = new ArrayList<>();

        if (!F.isEmpty(cfgs)) {
            for (ServiceConfiguration cfg : cfgs)
                res.add(new VisorServiceConfiguration(cfg));
        }

        return res;
    }

    /**
     * Default constructor.
     */
    public VisorServiceConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for service configuration.
     *
     * @param src Service configuration.
     */
    public VisorServiceConfiguration(ServiceConfiguration src) {
        name = src.getName();
        svc = compactClass(src.getService());
        totalCnt = src.getTotalCount();
        maxPerNodeCnt = src.getMaxPerNodeCount();
        cacheName = src.getCacheName();
        affKey = compactClass(src.getAffinityKey());
        nodeFilter = compactClass(src.getNodeFilter());
    }

    /**
     * @return Service name.
     */
    public String getName() {
        return name;
    }


    /**
     * @return Service instance.
     */
    public String getService() {
        return svc;
    }

    /**
     * @return Total number of deployed service instances in the cluster, {@code 0} for unlimited.
     */
    public int getTotalCount() {
        return totalCnt;
    }

    /**
     * @return Maximum number of deployed service instances on each node, {@code 0} for unlimited.
     */
    public int getMaxPerNodeCount() {
        return maxPerNodeCnt;
    }

    /**
     * @return Cache name, possibly {@code null}.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Affinity key, possibly {@code null}.
     */
    public String getAffinityKey() {
        return affKey;
    }

    /**
     * @return Node filter used to filter nodes on which the service will be deployed, possibly {@code null}.
     */
    public String getNodeFilter() {
        return nodeFilter;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeString(out, svc);
        out.writeInt(totalCnt);
        out.writeInt(maxPerNodeCnt);
        U.writeString(out, cacheName);
        U.writeString(out, affKey);
        U.writeString(out, nodeFilter);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        svc = U.readString(in);
        totalCnt = in.readInt();
        maxPerNodeCnt = in.readInt();
        cacheName = U.readString(in);
        affKey = U.readString(in);
        nodeFilter = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorServiceConfiguration.class, this);
    }
}
