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

/** Writes a class schema as JSON. */
public class WireJsonWriter {
    /** @return JSON of a schema. */
    public String write(Schema schema) {
        StringBuilder res = new StringBuilder("{\n  \"class\": ").append(quote(schema.cls()));

        if (!schema.annotations().isEmpty())
            res.append(",\n  \"annotations\": ").append(array(schema.annotations()));

        List<String> fields = new ArrayList<>();

        for (FieldRepresentation f : schema.fields()) {
            List<String> members = new ArrayList<>();

            if (f.order() != null)
                members.add("\"order\": " + f.order());

            members.add("\"type\": " + quote(f.type()));
            members.add("\"name\": " + quote(f.name()));

            if (!f.annotations().isEmpty())
                members.add("\"annotations\": " + array(f.annotations()));

            fields.add('{' + String.join(", ", members) + '}');
        }

        res.append(",\n  \"fields\": ").append(fields.isEmpty() ? "[]" : "[\n    " + String.join(",\n    ", fields) + "\n  ]");

        return res.append("\n}\n").toString();
    }

    /** @return JSON array of strings. */
    private String array(List<String> vals) {
        return vals.stream().map(this::quote).collect(Collectors.joining(", ", "[", "]"));
    }

    /** @return JSON string. */
    private String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }
}
