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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Comparator;

/** Compares nodes using the Data Center id as a primary factor. */
public class MdcAwareNodesComparator implements Comparator<TcpDiscoveryNode> {
    /** */
    @Override public int compare(TcpDiscoveryNode n1, TcpDiscoveryNode n2) {
        String n1DcId = n1.dataCenterId() == null ? "" : n1.dataCenterId();
        String n2DcId = n2.dataCenterId() == null ? "" : n2.dataCenterId();

        int res = n1DcId.compareTo(n2DcId);

        if (res == 0) {
            res = n1.compareTo(n2);
        }

        return res;
    }
}
