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
package collectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.sun.source.tree.CaseTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ConditionalExpressionTree;
import com.sun.source.tree.DoWhileLoopTree;
import com.sun.source.tree.EnhancedForLoopTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.ForLoopTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.IfTree;
import com.sun.source.tree.LambdaExpressionTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.StatementTree;
import com.sun.source.tree.SwitchTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.tree.WhileLoopTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the layouts of client messages and WAL records that Ignite reads and writes by hand. For each method that takes or
 * declares a reader or a writer, the value lists the operations on them in order, with the branches, loops and switch cases
 * around them. A method with a switch at its top describes each case apart, so a change of one WAL record or ODBC command
 * points at that case.
 * <p>
 * Items that start with {@code #} are structure: a branch with its condition, a loop, a switch case and the end of each. Items
 * that start with {@code ?} could not be resolved from the source: a call that gets the reader or writer, such as a helper or
 * the {@code writeBinary} of a field, and a method of a reader or writer that is not a read or write. A change that touches
 * such items or only structure is a possible layout change, which the reviewer checks; a change of the reads and writes is
 * certain. A method is identified by its class and
 * name: renaming a parameter type changes nothing, and overloads are described together. The class is a rename group, so
 * renaming an internal class with the same layout changes nothing. Adding or removing a method is only a possible change: it
 * may move code between helpers, and a new or removed message also changes its code, which {@link Codes} compares.
 */
public class Layouts implements Collector {
    /** Packages that read and write client messages and WAL records: area, source directory, whether its subpackages count. */
    private static final String[][] FAMILIES = {
        {"JDBC protocol", "modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/jdbc", "recursive"},
        {"ODBC protocol", "modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/odbc", "recursive"},
        {"JDBC and ODBC protocols", "modules/core/src/main/java/org/apache/ignite/internal/processors/odbc", "flat"},
        {"Thin client protocol", "modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client", "recursive"},
        {"Stored data", "modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/wal/serializer", "recursive"},
    };

    /** Simple names of the types that read or write the bytes of a message or record. */
    private static final Pattern IO =
        Pattern.compile("Binary\\w*(Reader|Writer)\\w*|\\w*(DataInput|DataOutput|InputStream|OutputStream)|ByteBuffer");

    /** Separator of the items of a value. */
    private static final String ITEMS = "; ";

    /** Flag of a value that lists items, some of which may be unresolved. */
    private static final String ITEMIZED = "itemized";

    /**
     * Operation that reads or writes bytes, such as {@code writeInt}, {@code doWriteString}, {@code putLong} or {@code get}: a
     * verb and the type it handles. Other methods of a reader or writer, such as {@code array} or {@code position}, are
     * unresolved: the check cannot tell whether a new name writes other bytes.
     */
    private static final Pattern OP = Pattern.compile("(?:do)?(read|write|put|get|skip)(\\w*)");

    /** */
    private static final JavaCompiler JAVAC = ToolProvider.getSystemJavaCompiler();

    /** */
    private static final StandardJavaFileManager FILES = JAVAC.getStandardFileManager(null, null, null);

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        for (String[] f : FAMILIES) {
            int found = describe(tree.resolve(f[1]), f[2].equals("recursive"), f[0], tree::relative, out);

            if (found == 0)
                out.error("No reading or writing methods found in " + f[1]);
        }
    }

    /**
     * Describes the methods of the source files in a directory.
     *
     * @param relative Path of a file as the description shows it.
     * @return Number of methods described.
     */
    static int describe(Path dir, boolean recursive, String area, java.util.function.Function<Path, String> relative, Declarations out)
        throws IOException {
        List<Path> files;

        try (Stream<Path> s = recursive ? Files.walk(dir) : Files.list(dir)) {
            files = s.filter(p -> p.toString().endsWith(".java")).sorted().collect(Collectors.toList());
        }

        int found = 0;

        for (Path f : files) {
            JavacTask task = (JavacTask)JAVAC.getTask(null, FILES, null, List.of("-proc:none"), null, FILES.getJavaFileObjects(f));

            for (CompilationUnitTree cu : task.parse()) {
                for (Tree t : cu.getTypeDecls()) {
                    if (t instanceof ClassTree) {
                        String top = ((ClassTree)t).getSimpleName().toString();

                        found += new Unit(cu, Trees.instance(task), area, relative.apply(f), top, out).classTree((ClassTree)t, top);
                    }
                }
            }
        }

        return found;
    }

    /** One source file. */
    private static final class Unit {
        /** */
        private final CompilationUnitTree cu;

        /** */
        private final Trees trees;

        /** */
        private final String area;

        /** */
        private final String file;

        /** Top-level class, the rename group. */
        private final String group;

        /** */
        private final Declarations out;

        /** */
        Unit(CompilationUnitTree cu, Trees trees, String area, String file, String group, Declarations out) {
            this.cu = cu;
            this.trees = trees;
            this.area = area;
            this.file = file;
            this.group = group;
            this.out = out;
        }

        /** Describes the methods of a class and of its nested classes. */
        int classTree(ClassTree cls, String path) {
            int found = 0;

            for (Tree m : cls.getMembers()) {
                if (m instanceof ClassTree)
                    found += classTree((ClassTree)m, path + '.' + ((ClassTree)m).getSimpleName());
                else if (m instanceof MethodTree && method((MethodTree)m, path, cls.getExtendsClause()))
                    found++;
            }

            return found;
        }

        /**
         * Describes a method that takes or declares a reader or writer.
         *
         * @param parent Superclass of the class, which a constructor may call with the reader or writer, or {@code null}.
         */
        boolean method(MethodTree m, String path, Tree parent) {
            if (m.getBody() == null)
                return false;

            Set<String> io = new HashSet<>();

            for (VariableTree p : m.getParameters()) {
                if (ioType(p.getType()))
                    io.add(p.getName().toString());
            }

            new TreeScanner<Void, Void>() {
                @Override public Void visitVariable(VariableTree v, Void unused) {
                    if (ioType(v.getType()))
                        io.add(v.getName().toString());

                    return super.visitVariable(v, unused);
                }
            }.scan(m.getBody(), null);

            if (io.isEmpty())
                return false;

            String key = "layout " + path + '.' + (m.getName().contentEquals("<init>") ? "new" : m.getName());
            String at = file + ':' + line(m);
            Items items = new Items(io, parent);

            for (StatementTree st : m.getBody().getStatements()) {
                if (st instanceof SwitchTree) {
                    SwitchTree sw = (SwitchTree)st;
                    List<String> labels = new ArrayList<>();

                    items.add("#switch:" + oneLine(sw.getExpression()), "#end");

                    // A case without statements falls through to the next one, so they are described together.
                    for (CaseTree c : sw.getCases()) {
                        labels.add(labels(c));

                        if (c.getStatements() != null && c.getStatements().isEmpty())
                            continue;

                        Items caseItems = new Items(io, parent);

                        caseItems.scan(c.getStatements() != null ? c.getStatements() : List.of(c.getBody()), null);
                        out.add(area, key + " case " + String.join(", ", labels), caseItems.text(), ITEMIZED, file + ':' + line(c), group,
                            null);
                        labels.clear();
                    }
                }
                else
                    items.scan(st, null);
            }

            out.add(area, key, items.text(), ITEMIZED, at, group, null);

            return true;
        }

        /** @return Line of a node. */
        private long line(Tree t) {
            return cu.getLineMap().getLineNumber(trees.getSourcePositions().getStartPosition(cu, t));
        }
    }

    /** @return Whether a declared type reads or writes the bytes of a message or record. */
    private static boolean ioType(Tree type) {
        if (type == null)
            return false;

        String name = type.toString().replaceAll("<.*>", "").replaceAll("\\[\\]", "");

        return IO.matcher(name.substring(name.lastIndexOf('.') + 1)).matches();
    }

    /** @return Labels of a case, or {@code default}. */
    private static String labels(CaseTree c) {
        return c.getExpressions().isEmpty() ? "default"
            : c.getExpressions().stream().map(Layouts::oneLine).collect(Collectors.joining(", "));
    }

    /** @return Source text of a node on one line. */
    private static String oneLine(Tree t) {
        return t.toString().replaceAll("\\s+", " ").trim();
    }

    /** Operations on the readers and writers of a method, with the structure around them. */
    private static final class Items extends TreeScanner<Void, Void> {
        /** Names of the readers and writers. */
        private final Set<String> io;

        /** Superclass whose constructor a constructor may call, or {@code null}. */
        private final Tree parent;

        /** */
        private final List<String> items = new ArrayList<>();

        /** */
        Items(Set<String> io, Tree parent) {
            this.io = io;
            this.parent = parent;
        }

        /** */
        void add(String... more) {
            items.addAll(List.of(more));
        }

        /** @return Items without the blocks that have no operations, such as a branch that only logs or checks. */
        String text() {
            return String.join(ITEMS, prune(items));
        }

        /** @return Items without the blocks that have no operations, in order. */
        private static List<String> prune(List<String> items) {
            List<String> res = new ArrayList<>();

            for (int i = 0; i < items.size(); i++) {
                String item = items.get(i);

                if (!item.startsWith("#") || item.equals("#end")) {
                    res.add(item);

                    continue;
                }

                int depth = 1;
                int end = i + 1;

                while (depth > 0) {
                    String next = items.get(end++);

                    depth += next.equals("#end") ? -1 : next.startsWith("#") ? 1 : 0;
                }

                List<String> inner = prune(items.subList(i + 1, end - 1));

                if (inner.stream().anyMatch(Items::operation)) {
                    res.add(item);
                    res.addAll(inner);
                    res.add("#end");
                }

                i = end - 1;
            }

            return res;
        }

        /** @return Whether an item reads or writes, or may: an operation or a call that gets the reader or writer. */
        private static boolean operation(String item) {
            return !item.startsWith("#");
        }

        /** @return Item of an operation of a reader or writer. */
        private static String op(String name) {
            java.util.regex.Matcher m = OP.matcher(name);

            if (!m.matches())
                return "?op:" + name;

            String verb = m.group(1).equals("put") ? "write" : m.group(1).equals("get") ? "read" : m.group(1);

            return m.group(2).isEmpty() ? verb : verb + ':' + m.group(2);
        }

        /** @return Whether an expression is one of the readers or writers, or a stream that one of them returns. */
        private boolean isIo(ExpressionTree e) {
            if (e instanceof IdentifierTree)
                return io.contains(((IdentifierTree)e).getName().toString());

            if (e instanceof MethodInvocationTree && ((MethodInvocationTree)e).getMethodSelect() instanceof MemberSelectTree)
                return isIo(((MemberSelectTree)((MethodInvocationTree)e).getMethodSelect()).getExpression());

            return false;
        }

        /** {@inheritDoc} */
        @Override public Void visitMethodInvocation(MethodInvocationTree node, Void unused) {
            ExpressionTree sel = node.getMethodSelect();
            String name = sel instanceof MemberSelectTree ? ((MemberSelectTree)sel).getIdentifier().toString() : sel.toString();
            ExpressionTree recv = sel instanceof MemberSelectTree ? ((MemberSelectTree)sel).getExpression() : null;

            if (recv != null && isIo(recv)) {
                scan(node.getArguments(), unused);
                items.add(op(name));

                return null;
            }

            if (recv instanceof IdentifierTree && ((IdentifierTree)recv).getName().contentEquals("super")) {
                scan(node.getArguments(), unused);
                items.add("super." + name);

                return null;
            }

            scan(recv, unused);
            scan(node.getArguments(), unused);

            if (node.getArguments().stream().anyMatch(this::isIo))
                items.add("?call:" + name + (name.equals("super") && parent != null ? " " + oneLine(parent) : ""));

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitNewClass(NewClassTree node, Void unused) {
            scan(node.getArguments(), unused);

            if (node.getArguments().stream().anyMatch(this::isIo))
                items.add("?call:new " + oneLine(node.getIdentifier()));

            scan(node.getClassBody(), unused);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitIf(IfTree node, Void unused) {
            items.add("#if:" + oneLine(node.getCondition()));
            scan(node.getCondition(), unused);
            scan(node.getThenStatement(), unused);
            items.add("#end");

            if (node.getElseStatement() != null) {
                items.add("#else");
                scan(node.getElseStatement(), unused);
                items.add("#end");
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitConditionalExpression(ConditionalExpressionTree node, Void unused) {
            items.add("#if:" + oneLine(node.getCondition()));
            scan(node.getCondition(), unused);
            scan(node.getTrueExpression(), unused);
            items.add("#end");
            items.add("#else");
            scan(node.getFalseExpression(), unused);
            items.add("#end");

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitSwitch(SwitchTree node, Void unused) {
            scan(node.getExpression(), unused);
            items.add("#switch:" + oneLine(node.getExpression()));

            for (CaseTree c : node.getCases()) {
                items.add("#case:" + labels(c));
                scan(c.getStatements() != null ? c.getStatements() : List.of(c.getBody()), unused);
                items.add("#end");
            }

            items.add("#end");

            return null;
        }

        /** Describes a loop: its condition, then its body. */
        private Void loop(Tree condition, Tree body, Void unused) {
            items.add("#loop");
            scan(condition, unused);
            scan(body, unused);
            items.add("#end");

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitForLoop(ForLoopTree node, Void unused) {
            scan(node.getInitializer(), unused);
            scan(node.getUpdate(), unused);

            return loop(node.getCondition(), node.getStatement(), unused);
        }

        /** {@inheritDoc} */
        @Override public Void visitEnhancedForLoop(EnhancedForLoopTree node, Void unused) {
            return loop(node.getExpression(), node.getStatement(), unused);
        }

        /** {@inheritDoc} */
        @Override public Void visitWhileLoop(WhileLoopTree node, Void unused) {
            return loop(node.getCondition(), node.getStatement(), unused);
        }

        /** {@inheritDoc} */
        @Override public Void visitDoWhileLoop(DoWhileLoopTree node, Void unused) {
            return loop(node.getCondition(), node.getStatement(), unused);
        }

        /** {@inheritDoc} */
        @Override public Void visitLambdaExpression(LambdaExpressionTree node, Void unused) {
            items.add("#lambda");
            scan(node.getBody(), unused);
            items.add("#end");

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visitClass(ClassTree node, Void unused) {
            return null;
        }
    }
}
