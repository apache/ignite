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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.w3c.dom.Element;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the artifacts of {@code ignite-bom}, the Java release that the build targets, the minimum Java version of the launch
 * scripts and the NuGet packages with their target frameworks.
 */
public class Distribution implements Collector {
    /** */
    private static final String AREA = "Distribution";

    /** Launch script that checks the Java version. */
    private static final String LAUNCHER = "bin/include/functions.sh";

    /** Check of the minimum Java version in the launch script. */
    private static final Pattern MIN_JAVA = Pattern.compile("\\$version -lt (\\d+)");

    /** Directory of the .NET projects. */
    private static final String DOTNET = "modules/platforms/dotnet";

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        for (BuiltTree.Artifact a : tree.bom())
            out.add(AREA, "ignite-bom: " + a.coordinates(), "", "", BuiltTree.BOM);

        out.add(AREA, "maven.compiler.release", BuiltTree.text(tree.xml(BuiltTree.PARENT).getDocumentElement(), "maven.compiler.release"),
            "", BuiltTree.PARENT);

        Matcher minJava = MIN_JAVA.matcher(Files.readString(tree.resolve(LAUNCHER)));

        if (!minJava.find())
            out.error("No minimum Java version check found in " + LAUNCHER);
        else
            out.add(AREA, "minimum Java version in launch scripts", minJava.group(1), "", LAUNCHER);

        try (Stream<Path> files = Files.walk(tree.resolve(DOTNET))) {
            for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".csproj"))::iterator) {
                Element project = BuiltTree.xml(f).getDocumentElement();
                String id = BuiltTree.text(project, "PackageId");

                if (id == null)
                    continue;

                String frameworks = BuiltTree.text(project, "TargetFrameworks");

                out.add(AREA, "NuGet package " + id, frameworks != null ? frameworks : BuiltTree.text(project, "TargetFramework"), "",
                    tree.relative(f));
            }
        }
    }
}
