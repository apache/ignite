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
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Ignite source tree built by describe.sh: the artifacts of {@code ignite-bom}, the classes and sources of the modules it
 * publishes, and the packages that the javadoc build publishes.
 */
class BuiltTree {
    /** POM of the BOM. */
    static final String BOM = "modules/bom/pom.xml";

    /** POM that sets the build properties. */
    static final String PARENT = "parent/pom.xml";

    /** Tree root. */
    private final Path root;

    /** Artifacts that the BOM declares, in its order. */
    private final List<Artifact> bom = new ArrayList<>();

    /** Class directories of the published modules built from this tree. */
    private final List<Path> classDirs;

    /** Package patterns that the javadoc build excludes. */
    private final List<String> excludedPkgs = new ArrayList<>();

    /** Sources of the published modules. */
    private final SourceIndex sources;

    /** @param root Tree root. */
    BuiltTree(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();

        List<Path> modules = publishedModules();

        classDirs = modules.stream().map(m -> m.resolve("target/classes")).filter(Files::isDirectory).collect(Collectors.toList());
        sources = new SourceIndex(this.root, modules);

        for (String p : text(xml("pom.xml").getDocumentElement(), "excludePackageNames").split("[:\\s]+")) {
            if (!p.isEmpty())
                excludedPkgs.add(p);
        }
    }

    /**
     * Artifact of the BOM, identified as in Maven: by group, name, type and classifier.
     *
     * @param classifier Classifier, or {@code null}.
     */
    record Artifact(String group, String name, String type, String classifier) {
        /** @return Coordinates as Maven writes them: the type only when it is not {@code jar} or there is a classifier. */
        String coordinates() {
            return group + ':' + name + (classifier != null ? ':' + type + ':' + classifier : "jar".equals(type) ? "" : ':' + type);
        }
    }

    /**
     * Reads the artifacts of {@code ignite-bom} as Maven publishes it: the build writes the BOM with resolved properties to the
     * file that {@code maven.flatten.file.name} names. The Java API comes from the main JAR of each module of this tree that the
     * BOM lists.
     *
     * @return Directories of the published modules built from this tree.
     */
    private List<Path> publishedModules() throws IOException {
        Map<String, Path> modules = new HashMap<>();

        try (Stream<Path> poms = Files.walk(root.resolve("modules"), 3)) {
            for (Path pom : (Iterable<Path>)poms.filter(p -> p.endsWith("pom.xml"))::iterator) {
                Element project = xml(pom).getDocumentElement();
                Element parent = (Element)project.getElementsByTagName("parent").item(0);
                String group = child(project, "groupId");

                if (group == null && parent != null)
                    group = child(parent, "groupId");

                modules.put(group + ':' + child(project, "artifactId"), pom.getParent());
            }
        }

        List<Path> res = new ArrayList<>();
        String flattened = text(xml(PARENT).getDocumentElement(), "maven.flatten.file.name");
        Element mgmt = (Element)xml(root.resolve(BOM).resolveSibling(flattened)).getElementsByTagName("dependencyManagement").item(0);
        NodeList deps = mgmt.getElementsByTagName("dependency");

        for (int i = 0; i < deps.getLength(); i++) {
            Element dep = (Element)deps.item(i);
            Artifact a = new Artifact(child(dep, "groupId"), child(dep, "artifactId"),
                Objects.requireNonNullElse(child(dep, "type"), "jar"), child(dep, "classifier"));
            Path module = modules.get(a.group + ':' + a.name);

            bom.add(a);

            if ("jar".equals(a.type) && a.classifier == null && module != null)
                res.add(module);
        }

        return res;
    }

    /** @return Artifacts that the BOM declares, in its order. */
    List<Artifact> bom() {
        return bom;
    }

    /** @return Path relative to the tree root. */
    String relative(Path p) {
        return root.relativize(p).toString();
    }

    /** @return Path of a file of the tree. */
    Path resolve(String rel) {
        return root.resolve(rel);
    }

    /** @return {@code True} if the javadoc build publishes the package. */
    boolean publicPackage(String pkg) {
        for (String p : excludedPkgs) {
            if (p.endsWith(".*") ? pkg.startsWith(p.substring(0, p.length() - 1)) : pkg.equals(p))
                return false;
        }

        return true;
    }

    /**
     * @param pkg Package, or an empty string for all packages.
     * @param publicOnly Whether to skip the packages that the javadoc build leaves out.
     * @return Classes of the package in the published modules, loaded without initialization. A class that cannot be loaded is
     *      an error of {@code out}.
     */
    List<Class<?>> classes(String pkg, boolean publicOnly, Declarations out) throws IOException {
        List<Class<?>> res = new ArrayList<>();

        for (Path dir : classDirs) {
            Path pkgDir = dir.resolve(pkg.replace('.', '/'));

            if (!Files.isDirectory(pkgDir))
                continue;

            try (Stream<Path> files = pkg.isEmpty() ? Files.walk(pkgDir) : Files.list(pkgDir)) {
                for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".class"))::iterator) {
                    String rel = dir.relativize(f).toString();
                    String name = rel.substring(0, rel.length() - ".class".length()).replace('/', '.');

                    if (name.endsWith("module-info") || name.endsWith("package-info")
                        || publicOnly && !publicPackage(name.substring(0, Math.max(0, name.lastIndexOf('.')))))
                        continue;

                    try {
                        res.add(Class.forName(name, false, BuiltTree.class.getClassLoader()));
                    }
                    catch (Throwable e) {
                        out.error("Failed to load " + name + ": " + e);
                    }
                }
            }
        }

        return res;
    }

    /** @return Class directories of the published modules. */
    List<Path> classDirs() {
        return classDirs;
    }

    /** @see SourceIndex#where */
    String where(Class<?> cls, String member, int params, boolean deprecated) {
        return sources.where(cls, member, params, deprecated);
    }

    /** @return Where the {@code @Deprecated} annotation of a deprecated declaration is, or {@code null} for other declarations. */
    String deprecationWhere(Class<?> cls, String member, int params, boolean deprecated) {
        return deprecated ? where(cls, member, params, true) : null;
    }

    /** @return Parsed XML file of the tree. */
    Document xml(String rel) throws IOException {
        return xml(root.resolve(rel));
    }

    /** @return Parsed XML file. A document type declaration is refused, because the file comes from the PR. */
    static Document xml(Path file) throws IOException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

            return factory.newDocumentBuilder().parse(file.toFile());
        }
        catch (ParserConfigurationException | SAXException e) {
            throw new IOException("Cannot parse " + file, e);
        }
    }

    /** @return Text of the first element with the name inside {@code scope}, or {@code null}. Comments are not text. */
    static String text(Element scope, String name) {
        NodeList found = scope.getElementsByTagName(name);

        return found.getLength() == 0 ? null : found.item(0).getTextContent().trim();
    }

    /** @return Text of the child element with the name, or {@code null}. */
    static String child(Element parent, String name) {
        for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n instanceof Element && n.getNodeName().equals(name))
                return n.getTextContent().trim();
        }

        return null;
    }
}
