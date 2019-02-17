/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;

/**
 */
public class ContentionInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ClusterNode node;

    /** */
    private List<String> entries;

    /**
     * @return Node.
     */
    public ClusterNode getNode() {
        return node;
    }

    /**
     * @param node Node.
     */
    public void setNode(ClusterNode node) {
        this.node = node;
    }

    /**
     * @return Entries.
     */
    public List<String> getEntries() {
        return entries;
    }

    /**
     * @param entries Entries.
     */
    public void setEntries(List<String> entries) {
        this.entries = entries;
    }

    /** */
    public void print() {
        System.out.println("[node=" + node + ']');

        for (String entry : entries)
            System.out.println("    " + entry);
    }
}

