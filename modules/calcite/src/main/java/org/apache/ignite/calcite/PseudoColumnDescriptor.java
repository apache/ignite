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

package org.apache.ignite.calcite;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;

/** Pseudocolumn descriptor. */
@IgniteExperimental
public interface PseudoColumnDescriptor {
    /** */
    int NOT_SPECIFIED = -1;

    /** @return Name of pseudocolumn. */
    String name();

    /** @return Pseudocolumn type. */
    Class<?> type();

    /** @return Scale of pseudocolumn type, {@value #NOT_SPECIFIED} if not specified. */
    int scale();

    /** @return Precision of pseudocolumn type, {@value #NOT_SPECIFIED} if not specified. */
    int precision();

    /**
     * @param ctx Context to extract value.
     * @return Value of a pseudocolumn.
     * @throws IgniteCheckedException If there are errors when getting value.
     */
    Object value(PseudoColumnValueExtractorContext ctx) throws IgniteCheckedException;
}
