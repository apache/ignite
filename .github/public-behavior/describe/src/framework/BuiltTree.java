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
import java.nio.file.Path;

/** Ignite source tree built by describe.sh, whose classes are on the class path of the check. */
public class BuiltTree {
    /** Sources of the classes of the tree. */
    private final SourceIndex sources;

    /** @param root Tree root. */
    public BuiltTree(Path root) throws IOException {
        sources = new SourceIndex(root.toRealPath());
    }

    /** @see SourceIndex#where */
    public String where(Class<?> cls, String field) {
        return sources.where(cls, field);
    }
}
