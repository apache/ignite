/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.sun.source.tree.*;
import java.util.*;
import java.util.function.Consumer;

/** Literal values used to write external binary objects and persistent files. */
class FormatContracts {
    private static final String STORAGE = "org.apache.ignite.internal.processors.cache.persistence.";
    private static final Set<String> BINARY_TAGS = Set.of(
        "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "CHAR", "BOOLEAN", "DECIMAL", "STRING", "UUID", "DATE",
        "BYTE_ARR", "SHORT_ARR", "INT_ARR", "LONG_ARR", "FLOAT_ARR", "DOUBLE_ARR", "CHAR_ARR", "BOOLEAN_ARR",
        "DECIMAL_ARR", "STRING_ARR", "UUID_ARR", "DATE_ARR", "OBJ_ARR", "COL", "MAP", "BINARY_OBJ", "ENUM", "ENUM_ARR",
        "TIMESTAMP", "TIMESTAMP_ARR", "TIME", "TIME_ARR", "NULL", "HANDLE", "OBJ");

    static void compare(String owner, ClassTree before, ClassTree after, Consumer<String> output) {
        Map<String, Number> old = fields(owner, before);
        for (var field : fields(owner, after).entrySet()) {
            Number from = old.get(field.getKey());
            if (from != null && !from.equals(field.getValue()))
                output.accept("Changed " + field.getKey() + " from " + from + " to " + field.getValue());
        }
    }

    private static Map<String, Number> fields(String owner, ClassTree cls) {
        Map<String, Number> values = new TreeMap<>();
        for (Tree member : cls.getMembers()) {
            if (owner.equals("org.apache.ignite.internal.pagemem.wal.record.WALRecord")
                && member instanceof ClassTree nested && nested.getKind() == Tree.Kind.ENUM
                && nested.getSimpleName().contentEquals("RecordType")) {
                for (Tree item : nested.getMembers()) {
                    if (item instanceof VariableTree entry && entry.getType().toString().equals("RecordType")
                        && entry.getInitializer() instanceof NewClassTree init && !init.getArguments().isEmpty()
                        && !entry.getName().toString().startsWith("RESERVED")
                        && JavaContracts.literal(init.getArguments().get(0)) instanceof Integer index)
                        values.put("WAL record index for " + entry.getName(), index);
                }
                continue;
            }
            if (!(member instanceof VariableTree field)
                || !field.getModifiers().getFlags().containsAll(Set.of(javax.lang.model.element.Modifier.STATIC,
                    javax.lang.model.element.Modifier.FINAL)))
                continue;
            String name = field.getName().toString();
            String subject = switch (owner) {
                case "org.apache.ignite.internal.binary.GridBinaryMarshaller" ->
                    BINARY_TAGS.contains(name) ? "binary type code for " + name :
                        name.equals("PROTO_VER") ? "binary object protocol version" : null;
                case STORAGE + "file.FilePageStore", STORAGE + "file.FilePageStoreV2" ->
                    Set.of("SIGNATURE", "VERSION").contains(name) ? cls.getSimpleName() + " header " + name : null;
                case STORAGE + "snapshot.SnapshotMetadata", STORAGE + "snapshot.IncrementalSnapshotMetadata" ->
                    name.equals("serialVersionUID") ? cls.getSimpleName() + " serialization version" : null;
                default -> null;
            };
            if (subject == null)
                continue;
            ExpressionTree expr = field.getInitializer();
            if (expr instanceof TypeCastTree cast && cast.getType().toString().equals("byte")
                && JavaContracts.literal(cast.getExpression()) instanceof Integer n)
                values.put(subject, (long)n.byteValue());
            else if (JavaContracts.literal(expr) instanceof Number n && (n instanceof Long || n instanceof Integer))
                values.put(subject, n.longValue());
        }
        return values;
    }
}
