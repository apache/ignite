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

package framework;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.Trees;

/** Finds where fields of the classes of the tree are declared in their sources. */
class SourceIndex {
    /** */
    private static final JavaCompiler JAVAC = ToolProvider.getSystemJavaCompiler();

    /** */
    private static final StandardJavaFileManager FILES = JAVAC.getStandardFileManager(null, null, null);

    /** Maven directory of the compiled classes of a module. */
    private static final Path CLASSES = Path.of("target", "classes");

    /** Maven directory of the sources of a module. */
    private static final Path SOURCES = Path.of("src", "main", "java");

    /** Tree root. */
    private final Path root;

    /** Lines of the fields of parsed source files, by file and then by {@link #classPath} and field name. */
    private final Map<Path, Map<String, Long>> parsed = new HashMap<>();

    /** @param root Real path of the tree root. */
    SourceIndex(Path root) {
        this.root = root;
    }

    /**
     * @return Source file and line of the field, the file alone if the file has no such field, or an empty string if the
     *      class was not compiled from the sources of the tree.
     */
    String where(Class<?> cls, String field) {
        Path src = source(cls);

        if (src == null)
            return "";

        Long line = parsed.computeIfAbsent(src, SourceIndex::parse).get(classPath(cls) + '#' + field);

        return root.relativize(src) + (line == null ? "" : ":" + line);
    }

    /**
     * @return Source file of the top-level class that contains {@code cls}, in the module whose compiled classes the class was
     *      loaded from, or {@code null}.
     */
    private Path source(Class<?> cls) {
        Class<?> top = cls;

        while (top.getDeclaringClass() != null)
            top = top.getDeclaringClass();

        CodeSource code = top.getProtectionDomain().getCodeSource();

        if (code == null)
            return null;

        try {
            Path classes = Path.of(code.getLocation().toURI()).toRealPath();

            if (!classes.startsWith(root) || !classes.endsWith(CLASSES))
                return null;

            Path src = classes.getParent().resolveSibling(SOURCES).resolve(top.getName().replace('.', '/') + ".java");

            return Files.isRegularFile(src) ? src : null;
        }
        catch (URISyntaxException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** @return Name of a class within its source file, such as {@code Outer.Inner}. */
    private static String classPath(Class<?> cls) {
        return cls.getDeclaringClass() == null ? cls.getSimpleName() : classPath(cls.getDeclaringClass()) + '.' + cls.getSimpleName();
    }

    /**
     * Parses a source file with the javac parser.
     *
     * @return Lines of the names of the fields of each class of the file, by {@link #classPath} and field name.
     */
    private static Map<String, Long> parse(Path src) {
        JavacTask task = (JavacTask)JAVAC.getTask(null, FILES, null, List.of("-proc:none"), null, FILES.getJavaFileObjects(src));

        try {
            CompilationUnitTree cu = task.parse().iterator().next();
            Source source = new Source(cu, cu.getSourceFile().getCharContent(true).toString(), Trees.instance(task).getSourcePositions());
            Map<String, Long> res = new HashMap<>();

            for (Tree t : cu.getTypeDecls()) {
                if (t instanceof ClassTree)
                    source.index((ClassTree)t, "", res);
            }

            return res;
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Source file parsed by javac. */
    private record Source(CompilationUnitTree cu, String text, SourcePositions pos) {
        /** Adds the fields of a class and of its nested classes. */
        void index(ClassTree cls, String outer, Map<String, Long> res) {
            String path = outer.isEmpty() ? cls.getSimpleName().toString() : outer + '.' + cls.getSimpleName();

            for (Tree t : cls.getMembers()) {
                if (t instanceof ClassTree)
                    index((ClassTree)t, path, res);
                else if (t instanceof VariableTree)
                    res.put(path + '#' + ((VariableTree)t).getName(), line((VariableTree)t));
            }
        }

        /** @return Line of the name of a field, which follows its modifiers and type. */
        long line(VariableTree f) {
            long start = pos.getStartPosition(cu, f);
            long from = Math.max(start, Math.max(pos.getEndPosition(cu, f.getModifiers()), pos.getEndPosition(cu, f.getType())));
            Matcher m = Pattern.compile("\\b" + Pattern.quote(f.getName().toString()) + "\\b").matcher(text);
            long at = m.find((int)from) && m.start() < pos.getEndPosition(cu, f) ? m.start() : start;

            return cu.getLineMap().getLineNumber(at);
        }
    }
}
