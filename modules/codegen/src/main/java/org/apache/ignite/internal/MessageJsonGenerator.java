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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.FileObject;
import org.apache.ignite.internal.wire.Schema;
import org.apache.ignite.internal.wire.SchemaReader;
import org.apache.ignite.internal.wire.WireJsonWriter;

import static javax.tools.StandardLocation.CLASS_OUTPUT;

/** Generates JSON representations of classes and of the enums their fields refer to. */
public class MessageJsonGenerator implements MessageGenerator {
    /** Directory of the representations in the class output. */
    private static final String WIRE_DIR = "META-INF/ignite-wire/";

    /** Enums described in each compilation, shared by its processors. */
    private static final Map<ProcessingEnvironment, Set<String>> DESCRIBED_ENUMS =
        Collections.synchronizedMap(new WeakHashMap<>());

    /** */
    private final ProcessingEnvironment env;

    /** */
    private final String dir;

    /** */
    private final SchemaReader reader;

    /** */
    private final WireJsonWriter writer = new WireJsonWriter();

    /** */
    private final Set<String> describedEnums;

    /**
     * @param env Processing environment.
     * @param dir Directory of the class representations.
     */
    public MessageJsonGenerator(ProcessingEnvironment env, String dir) {
        this.env = env;
        this.dir = dir;

        reader = new SchemaReader(env);
        describedEnums = DESCRIBED_ENUMS.computeIfAbsent(env, e -> new HashSet<>());
    }

    /** {@inheritDoc} */
    @Override public void generate(TypeElement type, List<VariableElement> fields) throws Exception {
        write(dir, reader.read(type, fields), type);

        for (Schema enumSchema : reader.enums(fields)) {
            if (describedEnums.add(enumSchema.cls()))
                write("enums/", enumSchema, type);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "JSON representation";
    }

    /** Writes a representation to the class output. */
    private void write(String dir, Schema schema, TypeElement src) throws IOException {
        FileObject file = env.getFiler().createResource(CLASS_OUTPUT, "", WIRE_DIR + dir + schema.cls() + ".json", src);

        try (Writer out = file.openWriter()) {
            out.write(writer.write(schema));
        }
    }
}
