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

import java.nio.file.Path;
import java.util.List;

/**
 * Prints the public declarations of a built Ignite source tree in the format that README.md describes. Descriptions, help
 * texts and examples are left out, so editing them changes nothing.
 * <p>
 * Usage: {@code java -cp <compiled check>:<classpath of the built tree> PublicDeclarations <tree root>}.
 */
public class PublicDeclarations {
    /** Collectors, one for each kind of public declarations. */
    private static final List<Collector> COLLECTORS = List.of(
        new JavaApi(),
        new SystemProperties(),
        new SystemViews(),
        new Cli(),
        new Codes(),
        new Sql(),
        new Distribution()
    );

    /** @param args Tree root. */
    public static void main(String[] args) throws Exception {
        BuiltTree tree = new BuiltTree(Path.of(args[0]));
        Declarations out = new Declarations();

        for (Collector c : COLLECTORS) {
            try {
                c.describe(tree, out);
            }
            catch (Exception | LinkageError e) {
                String file = c.getClass().getSimpleName() + ".java";

                throw new IllegalStateException(file + " could not describe the tree. If the PR changes the Ignite code that it " +
                    "reads, update .github/public-behavior/describe/" + file + " in the PR.", e);
            }
        }

        out.print(System.out);

        if (!out.errors().isEmpty()) {
            out.errors().forEach(System.err::println);

            System.exit(1);
        }
    }
}
