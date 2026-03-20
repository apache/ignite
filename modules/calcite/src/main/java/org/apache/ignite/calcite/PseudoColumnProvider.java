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

import java.util.List;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;

/**
 * Table pseudocolumn provider from {@link PluginProvider plugin} created via
 * {@link PluginProvider#createComponent(PluginContext, Class)} for Calcite-based query engine.
 */
@FunctionalInterface
@IgniteExperimental
public interface PseudoColumnProvider {
    /** */
    PseudoColumnProvider EMPTY = List::of;

    /**
     * Returns a list of pseudocolumn descriptions to add to tables.
     *
     * <p>NOTES:</p>
     * <ul>
     *     <li>When used in "SELECT", the pseudocolumn is applied to the table and not to the entire result.</li>
     *     <li>Added only for user tables, not for system views.</li>
     *     <li>{@link PseudoColumnDescriptor#name()} - it is recommended to return in uppercase.</li>
     *     <li>{@link PseudoColumnDescriptor#name()} - it is forbidden to use system names {@code "_KEY"} and
     *     {@code "_VAL"}.</li>
     *     <li>User will get an error when trying to create a column with name of one of pseudo ones.</li>
     *     <li>Updating or inserting into a pseudocolumn is prohibited.</li>
     * </ul>
     * @return Pseudocolumn descriptors to add to tables.
     */
    List<PseudoColumnDescriptor> provideDescriptors();
}
