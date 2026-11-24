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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Adds the declarations that describers of other languages printed, such as the C++ API. describe.sh runs a describer only when
 * the PR changes its platform, and then for both builds. A describer prints one declaration per line: area, key, value, flags
 * and location, separated by tabs. Its texts have no tabs or line breaks; the escaping is left to {@link Declarations}.
 */
public class Platforms implements Collector {
    /** Environment variable with the directory of the files that the describers printed. */
    private static final String DIR = "PUBLIC_BEHAVIOR_PLATFORMS";

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        String dir = System.getenv(DIR);

        if (dir == null)
            return;

        List<Path> files;

        try (Stream<Path> s = Files.list(Path.of(dir))) {
            files = s.filter(p -> p.toString().endsWith(".tsv")).sorted().collect(Collectors.toList());
        }

        for (Path f : files) {
            List<String> lines = Files.readAllLines(f);

            for (int i = 0; i < lines.size(); i++) {
                String[] cols = lines.get(i).split("\\t", -1);

                if (cols.length != 5) {
                    out.error(f.getFileName() + ":" + (i + 1) + " has " + cols.length + " columns instead of 5");

                    continue;
                }

                boolean deprecated = cols[3].contains("deprecated");

                out.add(cols[0], cols[1], cols[2], cols[3], cols[4], "", deprecated ? cols[4] : null);
            }

            if (lines.isEmpty())
                out.error(f.getFileName() + " has no declarations");
        }
    }
}
