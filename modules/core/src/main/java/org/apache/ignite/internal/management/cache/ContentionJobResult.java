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

package org.apache.ignite.internal.management.cache;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ContentionJobResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    ClusterNode node;

    /** */
    List<String> entries;

    /** */
    public ContentionJobResult(ClusterNode node, List<String> entries) {
        this.node = node;
        this.entries = entries;
    }

    /**
     * For externalization only.
     */
    public ContentionJobResult() {
    }

    /** */
    public void print(Consumer<String> printer) {
        printer.accept("[node=" + node + ']');

        for (String entry : entries)
            printer.accept("    " + entry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContentionJobResult.class, this);
    }
}

