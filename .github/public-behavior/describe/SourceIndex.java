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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.ModifiersTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.Trees;

/** Finds where classes and their members are declared in the sources of the published modules. */
class SourceIndex {
    /** */
    private static final JavaCompiler JAVAC = ToolProvider.getSystemJavaCompiler();

    /** */
    private static final StandardJavaFileManager FILES = JAVAC.getStandardFileManager(null, null, null);

    /** Tree root. */
    private final Path root;

    /** Source files of top-level classes. */
    private final Map<String, Path> sources = new HashMap<>();

    /** Declarations of source files parsed so far. */
    private final Map<Path, Map<String, Map<String, List<Decl>>>> parsed = new HashMap<>();

    /** Maps top-level classes of the modules to their source files. */
    SourceIndex(Path root, List<Path> modules) throws IOException {
        this.root = root;

        for (Path module : modules) {
            Path src = module.resolve("src/main/java");

            if (!Files.isDirectory(src))
                continue;

            try (Stream<Path> files = Files.walk(src)) {
                for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    String rel = src.relativize(f).toString();

                    sources.put(rel.substring(0, rel.length() - ".java".length()).replace('/', '.'), f);
                }
            }
        }
    }

    /**
     * @param cls Class.
     * @param member Member name, or {@code null} for the class itself. A constructor has the name of its class.
     * @param params Number of parameters of a method or constructor, or -1 for a field or the class.
     * @param deprecated Whether to point at the {@code @Deprecated} annotation of the declaration.
     * @return Source file and line of the declaration, or the file alone if the line is not certain, or an empty string if
     *      the class has no source file. If {@code deprecated}, the line is that of its {@code @Deprecated}.
     */
    String where(Class<?> cls, String member, int params, boolean deprecated) {
        Class<?> top = cls;

        while (top.getDeclaringClass() != null)
            top = top.getDeclaringClass();

        Path src = sources.get(top.getName());

        if (src == null)
            return "";

        String file = root.relativize(src).toString();
        Map<String, List<Decl>> members = parsed.computeIfAbsent(src, SourceIndex::parse).get(classPath(cls));

        if (members == null)
            return file;

        List<Decl> found = members.getOrDefault(member == null ? "" : member, List.of()).stream()
            .filter(d -> d.params == params && (!deprecated || d.deprecated > 0)).collect(Collectors.toList());

        if (found.size() == 1)
            return file + ':' + (deprecated ? found.get(0).deprecated : found.get(0).line);

        return deprecated ? file : file + ':' + members.get("").get(0).line;
    }

    /** @return Name of a class within its source file, such as {@code Outer.Inner}. */
    private static String classPath(Class<?> cls) {
        return cls.getDeclaringClass() == null ? cls.getSimpleName() : classPath(cls.getDeclaringClass()) + '.' + cls.getSimpleName();
    }

    /**
     * Parses a source file with the javac parser.
     *
     * @return Declarations of each class of the file by its {@link #classPath}, and then by member name. The class itself has
     *      the empty name.
     */
    private static Map<String, Map<String, List<Decl>>> parse(Path src) {
        JavacTask task = (JavacTask)JAVAC.getTask(null, FILES, null, List.of("-proc:none"), null, FILES.getJavaFileObjects(src));

        try {
            CompilationUnitTree cu = task.parse().iterator().next();
            Source source = new Source(cu, cu.getSourceFile().getCharContent(true).toString(), Trees.instance(task).getSourcePositions());
            Map<String, Map<String, List<Decl>>> res = new HashMap<>();

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
        /** Adds the declarations of a class and of its nested classes. */
        void index(ClassTree cls, String outer, Map<String, Map<String, List<Decl>>> res) {
            String name = cls.getSimpleName().toString();
            String path = outer.isEmpty() ? name : outer + '.' + name;
            Map<String, List<Decl>> members = res.computeIfAbsent(path, k -> new HashMap<>());

            members.computeIfAbsent("", k -> new ArrayList<>()).add(decl(cls, cls.getModifiers(), null, name, -1));

            for (Tree t : cls.getMembers()) {
                if (t instanceof ClassTree)
                    index((ClassTree)t, path, res);
                else if (t instanceof MethodTree) {
                    MethodTree m = (MethodTree)t;
                    String member = m.getName().contentEquals("<init>") ? name : m.getName().toString();

                    members.computeIfAbsent(member, k -> new ArrayList<>())
                        .add(decl(m, m.getModifiers(), m.getReturnType(), member, m.getParameters().size()));
                }
                else if (t instanceof VariableTree) {
                    VariableTree f = (VariableTree)t;
                    String member = f.getName().toString();

                    members.computeIfAbsent(member, k -> new ArrayList<>()).add(decl(f, f.getModifiers(), f.getType(), member, -1));
                }
            }
        }

        /**
         * @param type Type of a field or return type of a method, or {@code null}. The name follows it.
         * @return Declaration whose line is that of its name.
         */
        Decl decl(Tree decl, ModifiersTree mods, Tree type, String name, int params) {
            long start = pos.getStartPosition(cu, decl);
            long from = Math.max(start, Math.max(pos.getEndPosition(cu, mods), type == null ? -1 : pos.getEndPosition(cu, type)));
            Matcher m = Pattern.compile("\\b" + Pattern.quote(name) + "\\b").matcher(text);
            long at = m.find((int)from) && m.start() < pos.getEndPosition(cu, decl) ? m.start() : start;

            long deprecated = mods.getAnnotations().stream()
                .filter(a -> a.getAnnotationType().toString().matches("(java\\.lang\\.)?Deprecated"))
                .mapToLong(a -> line(pos.getStartPosition(cu, a))).findFirst().orElse(-1);

            return new Decl(params, line(at), deprecated);
        }

        /** @return Line of a position in the file. */
        long line(long at) {
            return cu.getLineMap().getLineNumber(at);
        }
    }

    /**
     * Declaration in a source file.
     *
     * @param params Number of parameters of a method or constructor, or -1 for a field or a class.
     * @param line Line of its name.
     * @param deprecated Line of its {@code @Deprecated}, or -1.
     */
    private record Decl(int params, long line, long deprecated) {}
}
