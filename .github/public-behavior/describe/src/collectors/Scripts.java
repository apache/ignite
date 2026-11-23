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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the launch scripts of the distribution and the options of {@code ignite.sh}, which its argument parser declares as
 * the cases it handles.
 */
public class Scripts implements Collector {
    /** Directory of the launch scripts. */
    private static final String BIN = "bin";

    /** Argument parser of {@code ignite.sh}. */
    private static final String PARSER = "bin/include/parseargs.sh";

    /** Case of the argument parser that handles an option, such as {@code -v)} or {@code -J*)}. */
    private static final Pattern OPTION = Pattern.compile("^\\s*(-[A-Za-z]+\\*?)\\)");

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        try (Stream<Path> files = Files.list(tree.resolve(BIN))) {
            for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".sh") || p.toString().endsWith(".bat"))::iterator)
                out.add("Distribution", "script " + tree.relative(f), "", "", tree.relative(f));
        }

        List<String> lines = Files.readAllLines(tree.resolve(PARSER));
        int found = 0;

        for (int i = 0; i < lines.size(); i++) {
            Matcher m = OPTION.matcher(lines.get(i));

            if (m.find()) {
                out.add("CLI", "ignite.sh " + m.group(1), "option", "", PARSER + ':' + (i + 1));

                found++;
            }
        }

        if (found == 0)
            out.error("No options found in " + PARSER);
    }
}
