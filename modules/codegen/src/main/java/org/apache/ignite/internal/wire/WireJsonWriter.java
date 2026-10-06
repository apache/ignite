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

package org.apache.ignite.internal.wire;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes a class schema as JSON. The output is compared byte by byte, so it is stable: the order of the keys is fixed,
 * a key without a value is omitted, lines are separated with {@code \n} on every platform.
 */
public class WireJsonWriter {
    /** @return Content of the JSON file of a schema: a field per line. */
    public String write(Schema schema) {
        StringBuilder res = new StringBuilder("{\n  \"class\": ").append(quote(schema.cls()));

        if (!schema.annotations().isEmpty())
            res.append(",\n  \"annotations\": ").append(annotations(schema.annotations(), ",\n    ", "[\n    ", "\n  ]"));

        List<String> fields = new ArrayList<>();

        for (FieldRepresentation f : schema.fields()) {
            List<String> members = new ArrayList<>();

            if (f.order() != null)
                members.add("\"order\": " + f.order());

            if (f.type() != null)
                members.add("\"type\": " + quote(f.type()));

            members.add("\"name\": " + quote(f.name()));

            if (!f.annotations().isEmpty())
                members.add("\"annotations\": " + annotations(f.annotations(), ", ", "[", "]"));

            fields.add('{' + String.join(", ", members) + '}');
        }

        res.append(",\n  \"fields\": ").append(fields.isEmpty() ? "[]" : "[\n    " + String.join(",\n    ", fields) + "\n  ]");

        return res.append("\n}\n").toString();
    }

    /** @return JSON array of annotations. */
    private String annotations(List<String> annotations, String sep, String prefix, String suffix) {
        return annotations.stream().map(this::quote).collect(Collectors.joining(sep, prefix, suffix));
    }

    /** @return JSON string. The values are names and annotations from the code, so only a quote and a backslash are escaped. */
    private String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }
}
