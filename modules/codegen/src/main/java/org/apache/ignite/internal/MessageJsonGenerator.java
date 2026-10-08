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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.wire.Schema;
import org.apache.ignite.internal.wire.SchemaReader;
import org.apache.ignite.internal.wire.WireJsonWriter;

/** Generates JSON representations of messages and of the enums their fields refer to. */
public class MessageJsonGenerator implements MessageGenerator {
    /** Directory of the representations in the class output. */
    private static final String WIRE_DIR = "META-INF/ignite-wire/";

    /** */
    private final ProcessingEnvironment env;

    /** */
    private final SchemaReader reader;

    /** */
    private final WireJsonWriter writer = new WireJsonWriter();

    /** */
    MessageJsonGenerator(ProcessingEnvironment env) {
        this.env = env;

        reader = new SchemaReader(env);
    }

    /** {@inheritDoc} */
    @Override public void generate(TypeElement type, List<VariableElement> fields) throws Exception {
        write("messages/", reader.read(type, fields), type);

        for (Schema enumSchema : reader.enums(fields)) {
            try {
                write("enums/", enumSchema, type);
            }
            catch (FilerException ignored) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "JSON representation";
    }

    /** Writes a representation to the class output. */
    private void write(String dir, Schema schema, TypeElement src) throws IOException {
        try (OutputStream out = env.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", WIRE_DIR + dir + schema.cls() + ".json", src).openOutputStream()) {
            out.write(writer.write(schema).getBytes(StandardCharsets.UTF_8));
        }
    }
}
