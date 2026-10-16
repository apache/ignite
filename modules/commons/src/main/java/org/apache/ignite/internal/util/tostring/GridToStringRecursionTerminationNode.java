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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * A node that terminates recursion by outputting a class name and object identity.
 */
class GridToStringRecursionTerminationNode extends GridToStringNode {
    /** The simple name of the class of the terminated object. */
    private final String simpleClsName;

    /** The identity hash code string of the terminated object. */
    private final String identity;

    /**
     * Private constructor to create a termination node.
     * @param obj The object that caused the recursion.
     */
    private GridToStringRecursionTerminationNode(Object obj) {
        super(null);
        simpleClsName = obj.getClass().getSimpleName();
        identity = GridToStringBuilder.identity(obj);
    }

    /**
     * Factory method to create a termination node and mark the monitor.
     * @param nodeRecursionMonitor The monitor for the current object.
     * @param obj The object that is being re-encountered.
     * @return A new termination node.
     */
    static GridToStringNode of(NodeRecursionMonitor nodeRecursionMonitor, Object obj) {
        nodeRecursionMonitor.setHashRequired();
        return new GridToStringRecursionTerminationNode(obj);
    }

    /** {@inheritDoc} */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        sb.a(simpleClsName).a(identity);
    }
}
