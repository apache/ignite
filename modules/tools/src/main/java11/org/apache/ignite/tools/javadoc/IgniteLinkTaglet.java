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

package org.apache.ignite.tools.javadoc;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;
import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.UnknownInlineTagTree;
import com.sun.source.util.SimpleDocTreeVisitor;
import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Taglet;

/**
 * Represents {@ignitelink Class} tag. This tag can
 * be used as replacement of {@link Class} tag that references to the Ignite class that is not in classpath.
 * Class and its arguments should have fully qualified names.
 */
public class IgniteLinkTaglet implements Taglet {
    /** */
    private static final String NAME = "ignitelink";

    /** */
    private DocletEnvironment env;

    /** {@inheritDoc} */
    @Override public void init(DocletEnvironment env, Doclet doclet) {
        this.env = env;
    }

    /**
     * Return the name of this custom tag.
     */
    @Override public String getName() {
        return NAME;
    }

    /** {@inheritDoc} */
    @Override public Set<Location> getAllowedLocations() {
        return new HashSet<>();
    }

    /**
     * Will return true since this is an inline tag.
     *
     * @return true since this is an inline tag.
     */
    @Override public boolean isInlineTag() {
        return true;
    }

    /**
     * Given the <code>DocTree</code> representation of this custom tag, return its string representation.
     * <p>
     * Input: org.apache.ignite.grid.spi.indexing.h2.GridH2IndexingSpi#setIndexCustomFunctionClasses(Class[])
     * <p>
     * Output: &lt;a href="../../../../../org/apache/ignite/grid/spi/indexing/h2/GridH2IndexingSpi.html#
     * setIndexCustomFunctionClasses(java.lang.Class...)"&gt;
     * &lt;code&gt;GridH2IndexingSpi.setIndexCustomFunctionClasses(java.lang.Class[])&lt;/code&gt;&lt;/a&gt;
     *
     * @param tags <code>DocTree</code> representation of this custom tag.
     * @param element The element to which the enclosing comment belongs.
     */
    @Override public String toString(List<? extends DocTree> tags, Element element) {
        for (DocTree tag : tags) {
            String text = new SimpleDocTreeVisitor<String, Void>() {
                @Override public String visitUnknownInlineTag(UnknownInlineTagTree node, Void param) {
                    return node.getContent().toString();
                }
            }.visit(tag, null);

            if (text == null || text.isEmpty())
                return "";

            File f = new File(env.getDocTrees().getPath(element).getCompilationUnit().getSourceFile().toUri());

            String curCls = f == null ? "" : f.getAbsolutePath().replace(File.separator, ".");

            String packPref = "src.main.java.";

            int idx = curCls.indexOf(packPref);

            StringBuilder path = new StringBuilder();

            if (idx != -1) {
                curCls = curCls.substring(idx + packPref.length());

                for (int i = 0, n = curCls.split("\\.").length - 2; i < n; i++)
                    path.append("../");
            }

            String[] tokens = text.split("#");

            int lastIdx = tokens[0].lastIndexOf('.');

            String simpleClsName = lastIdx != -1 && lastIdx + 1 < tokens[0].length() ?
                tokens[0].substring(lastIdx + 1) : tokens[0];

            String fullyQClsName = tokens[0].replace(".", "/");

            return "<a href=\"" + path + fullyQClsName + ".html" +
                (tokens.length > 1 ? ("#" + tokens[1].replace("[]", "...")) : "") +
                "\"><code>" + simpleClsName + (tokens.length > 1 ? ("." + tokens[1]) : "") + "</code></a>";
        }

        return "";
    }
}
