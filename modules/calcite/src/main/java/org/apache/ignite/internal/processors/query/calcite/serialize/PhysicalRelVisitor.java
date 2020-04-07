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

package org.apache.ignite.internal.processors.query.calcite.serialize;

/**
 *
 */
public interface PhysicalRelVisitor<T> {
    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(TableScanPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(FilterPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(HashFilterPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(ProjectPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(JoinPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(ReceiverPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(ValuesPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(SenderPhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(AggregatePhysicalRel rel);

    /**
     * See {@link PhysicalRelVisitor#visit(PhysicalRel)}
     */
    T visit(TableModifyPhysicalRel rel);

    /**
     * Processes given physical rel.
     *
     * @param rel Physical rel.
     * @return Visit result.
     */
    T visit(PhysicalRel rel);
}
