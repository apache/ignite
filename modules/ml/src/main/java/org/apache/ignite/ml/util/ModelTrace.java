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

package org.apache.ignite.ml.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Helper for model tracing.
 */
public class ModelTrace {
    /** Model name. */
    private final String mdlName;
    /** Use pretty mode. */
    private final boolean pretty;
    /** Model fields. */
    private List<IgniteBiTuple<String, Object>> mdlFields = new ArrayList<>();

    /**
     * Creates an instance of ModelTrace.
     *
     * @param mdlName Model name.
     * @param pretty Pretty.
     */
    public static ModelTrace builder(String mdlName, boolean pretty) {
        return new ModelTrace(mdlName, pretty);
    }

    /**
     * Creates an instance of ModelTrace.
     *
     * @param mdlName Model name.
     */
    public static ModelTrace builder(String mdlName) {
        return new ModelTrace(mdlName, false);
    }

    /**
     * Creates an instance of ModelTrace.
     *
     * @param mdlName Model name.
     * @param pretty Pretty.
     */
    private ModelTrace(String mdlName, boolean pretty) {
        this.mdlName = mdlName;
        this.pretty = pretty;
    }

    /**
     * Add field.
     *
     * @param name Name.
     * @param val Value.
     */
    public ModelTrace addField(String name, String val) {
        mdlFields.add(new IgniteBiTuple<>(name, val));
        return this;
    }

    /**
     * Add field.
     *
     * @param name Name.
     * @param values Values.
     */
    public ModelTrace addField(String name, List values) {
        mdlFields.add(new IgniteBiTuple<>(name, values));
        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder builder = new StringBuilder();

        final String ending = pretty ? "\n" : "";
        final String fields = mdlFields.stream()
            .map(kv -> fieldToString(kv, pretty))
            .collect(Collectors.joining(pretty ? ",\n" : ", "));

        builder.append(mdlName)
            .append(" [").append(ending)
            .append(fields)
            .append(ending).append("]");

        return builder.toString();
    }

    /**
     * Convert Field Name-Value pair to string.
     *
     * @param kv Field name and value.
     * @param pretty Use pretty mode.
     */
    @NotNull private String fieldToString(IgniteBiTuple<String, Object> kv, boolean pretty) {
        StringBuilder builder = new StringBuilder(pretty ? "\t" : "")
            .append(kv.getKey()).append(" = [");
        if (kv.getValue() instanceof List) {
            List list = (List)kv.getValue();
            builder
                .append(pretty ? "\n" : "")
                .append(list.stream()
                    .map(x -> (pretty ? "\t\t" : "") + x)
                    .collect(Collectors.joining(pretty ? ",\n" : ", ")))
                .append(pretty ? "\n\t]" : "]");
        }
        else {
            builder.append(kv.getValue().toString())
                .append("]");
        }
        return builder.toString();
    }
}
