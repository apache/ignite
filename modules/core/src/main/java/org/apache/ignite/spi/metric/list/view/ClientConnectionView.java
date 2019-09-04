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

package org.apache.ignite.spi.metric.list.view;

import java.net.InetSocketAddress;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;

/**
 * Client connection representation for a {@link MonitoringList}.
 */
public interface ClientConnectionView extends MonitoringRow<Long> {
    /** {@inheritDoc} */
    @Override default Long monitoringRowId() {
        return connectionId();
    }

    /** */
    public long connectionId();

    /** */
    public String type();

    /** */
    public InetSocketAddress localAddress();

    /** */
    public InetSocketAddress remoteAddress();

    /** */
    public String user();

    /** */
    public String version();
}
