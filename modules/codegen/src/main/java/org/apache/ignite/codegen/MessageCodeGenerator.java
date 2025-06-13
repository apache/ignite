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

package org.apache.ignite.codegen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;
import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
* Direct marshallable code generator.
*/
public class MessageCodeGenerator {
    /** */
    public static final String DFLT_SRC_DIR = U.getIgniteHome() + "/modules/core/src/main/java";

    /** */
    public static final String INDEXING_SRC_DIR = U.getIgniteHome() + "/modules/indexing/src/main/java";

    /** */
    public static final String CALCITE_SRC_DIR = U.getIgniteHome() + "/modules/calcite/src/main/java";

    /** */
    private static final Class<?> BASE_CLS = Message.class;

    /** */
    public static final String TAB = "    ";

    /** */
    private final Collection<String> write = new ArrayList<>();

    /** */
    private final Collection<String> read = new ArrayList<>();

    /** */
    private final String srcDir;

    /** */
    private int indent;

    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        String srcDir = DFLT_SRC_DIR;

        if (args != null && args.length > 0)
            srcDir = args[0];

        MessageCodeGenerator gen = new MessageCodeGenerator(srcDir);

        // TODO: generate all?
        gen.generateAndWrite(GridJobCancelRequest.class);
    }

    /**
     * @param srcDir Source directory.
     */
    public MessageCodeGenerator(String srcDir) {
        this.srcDir = srcDir;
    }

    /**
     * Generates code for all classes.
     *
     * @param write Whether to write to file.
     * @throws Exception In case of error.
     */
    public void generateAll(boolean write) throws Exception {
        Collection<Class<? extends Message>> classes = classes();

        for (Class<? extends Message> cls : classes) {
            try {
                boolean isAbstract = Modifier.isAbstract(cls.getModifiers());

                System.out.println("Processing class: " + cls.getName() + (isAbstract ? " (abstract)" : ""));

                if (write)
                    generateAndWrite(cls);
                else
                    generate(cls);
            }
            catch (IllegalStateException e) {
                System.out.println("Will skip class generation [cls=" + cls + ", err=" + e.getMessage() + ']');
            }
        }
    }

    /**
     * Generates code for provided class and writes it to source file.
     * Note: this method must be called only from {@code generateAll(boolean)}
     * and only with updating {@code CLASSES_ORDER_FILE} and other auto generated files.
     *
     * @param cls Class.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("ConstantConditions")
    public void generateAndWrite(Class<? extends Message> cls) throws Exception {
        assert cls != null;

        generate(cls);

        File file = new File(srcDir, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile()) {
            System.out.println("Source file not found: " + file.getPath());

            return;
        }

        Collection<String> src = new ArrayList<>();

        BufferedReader rdr = null;

        try {
            rdr = new BufferedReader(new FileReader(file));

            String line;
            boolean skip = false;

            boolean writeFound = false;
            boolean readFound = false;

            while ((line = rdr.readLine()) != null) {
                if (!skip) {
                    src.add(line);

                    if (line.contains("public boolean writeTo(ByteBuffer buf, MessageWriter writer)")) {
                        src.addAll(write);

                        skip = true;

                        writeFound = true;
                    }
                    else if (line.contains("public boolean readFrom(ByteBuffer buf, MessageReader reader)")) {
                        src.addAll(read);

                        skip = true;

                        readFound = true;
                    }
                }
                else if (line.startsWith(TAB + "}")) {
                    src.add(line);

                    skip = false;
                }
            }

            if (!writeFound)
                System.out.println("    writeTo method doesn't exist.");

            if (!readFound)
                System.out.println("    readFrom method doesn't exist.");
        }
        finally {
            if (rdr != null)
                rdr.close();
        }

        BufferedWriter wr = null;

        try {
            wr = new BufferedWriter(new FileWriter(file));

            for (String line : src)
                wr.write(line + '\n');
        }
        finally {
            if (wr != null)
                wr.close();
        }
    }

    /**
     * Generates code for provided class.
     *
     * @param cls Class.
     */
    private void generate(Class<? extends Message> cls) {
        assert cls != null;

        if (cls.isInterface())
            return;

        if (cls.isAnnotationPresent(IgniteCodeGeneratingFail.class))
            throw new IllegalStateException("@IgniteCodeGeneratingFail is provided for class: " + cls.getName());

        write.clear();
        read.clear();

        indent = 2;

        SB wb = builder();
        wb.a("return ").a(cls.getSimpleName()).a("Serializer.writeTo(this, buf, writer);");
        write.add(wb.toString());

        SB rb = builder();
        rb.a("return ").a(cls.getSimpleName()).a("Serializer.readFrom(this, buf, reader);");
        read.add(rb.toString());
    }

    /**
     * Creates new builder with correct indent.
     *
     * @return Builder.
     */
    private SB builder() {
        assert indent > 0;

        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        return sb;
    }

    /**
     * Gets all direct marshallable classes.
     * First classes will be classes from {@code classesOrder} with same order
     * as ordered values. Other classes will be at the end and ordered by name
     * (with package prefix).
     * That orders need for saving {@code directType} value.
     *
     * @return Classes.
     * @throws Exception In case of error.
     */
    private Collection<Class<? extends Message>> classes() throws Exception {
        Collection<Class<? extends Message>> col = new TreeSet<>(
            new Comparator<Class<? extends Message>>() {
                @Override public int compare(Class<? extends Message> c1,
                    Class<? extends Message> c2) {
                    return c1.getName().compareTo(c2.getName());
                }
            });

        ClassLoader ldr = getClass().getClassLoader();

        for (URL url : IgniteUtils.classLoaderUrls(ldr)) {
            File file = new File(url.toURI());

            int prefixLen = file.getPath().length() + 1;

            processFile(file, ldr, prefixLen, col);
        }

        return col;
    }

    /**
     * Recursively process provided file or directory.
     *
     * @param file File.
     * @param ldr Class loader.
     * @param prefixLen Path prefix length.
     * @param col Classes.
     * @throws Exception In case of error.
     */
    private void processFile(File file, ClassLoader ldr, int prefixLen,
        Collection<Class<? extends Message>> col) throws Exception {
        assert file != null;
        assert ldr != null;
        assert prefixLen > 0;
        assert col != null;

        if (!file.exists())
            throw new FileNotFoundException("File doesn't exist: " + file);

        if (file.isDirectory()) {
            for (File f : file.listFiles())
                processFile(f, ldr, prefixLen, col);
        }
        else {
            assert file.isFile();

            String path = file.getPath();

            if (path.endsWith(".class")) {
                String clsName = path.substring(prefixLen, path.length() - 6).replace(File.separatorChar, '.');

                Class<?> cls = Class.forName(clsName, false, ldr);

                if (cls.getDeclaringClass() == null && cls.getEnclosingClass() == null &&
                    !BASE_CLS.equals(cls) && BASE_CLS.isAssignableFrom(cls))
                    col.add((Class<? extends Message>)cls);
            }
        }
    }
}
