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
     * @param value Value.
     */
    public ModelTrace addField(String name, String value) {
        mdlFields.add(new IgniteBiTuple<>(name, value));
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
