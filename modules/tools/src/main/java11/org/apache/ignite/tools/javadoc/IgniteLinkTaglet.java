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

import com.sun.javadoc.Tag;
import com.sun.source.doctree.DocTree;
import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.lang.model.element.Element;
import jdk.javadoc.doclet.Taglet;

/**
 * Represents {@ignitelink Class} tag. This tag can
 * be used as replacement of {@link Class} tag that references to the Ignite class that is not in classpath.
 * Class and its arguments should have fully qualified names.
 */
public class IgniteLinkTaglet implements Taglet {
    /** */
    private static final String NAME = "ignitelink";

    /**
     * Return the name of this custom tag.
     */
    @Override public String getName() {
        return NAME;
    }

    @Override public String toString(List<? extends DocTree> tags, Element element) {
        StringBuilder sb = new StringBuilder();

        for (Iterator<? extends DocTree> iter = tags.iterator(); iter.hasNext(); ) {
            DocTree next = iter.next();

            sb.append(""); //todo IGNITE-11393 Implement toString for Java 9+
        }

        return sb.toString();
    }

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
     * Register this Taglet.
     *
     * @param tagletMap the map to register this tag to.
     */
    public static void register(Map<String, IgniteLinkTaglet> tagletMap) {
        IgniteLinkTaglet tag = new IgniteLinkTaglet();

        Taglet t = tagletMap.get(tag.getName());

        if (t != null)
            tagletMap.remove(tag.getName());

        tagletMap.put(tag.getName(), tag);
    }

    /**
     * Given the <code>Tag</code> representation of this custom tag, return its string representation.
     * <p>
     * Input: org.apache.ignite.grid.spi.indexing.h2.GridH2IndexingSpi#setIndexCustomFunctionClasses(Class[])
     * <p>
     * Output: <a href="../../../../../org/apache/ignite/grid/spi/indexing/h2/GridH2IndexingSpi.html#
     * setIndexCustomFunctionClasses(java.lang.Class...)">
     * <code>GridH2IndexingSpi.setIndexCustomFunctionClasses(java.lang.Class[])</code></a>
     *
     * @param tag <code>Tag</code> representation of this custom tag.
     */
     public String toString(Tag tag) {
        if (tag.text() == null || tag.text().isEmpty())
            return "";

        File f = tag.position().file();

        String curClass = f == null ? "" : f.getAbsolutePath().replace(File.separator, ".");

        String packPref = "src.main.java.";

        int idx = curClass.indexOf(packPref);

        StringBuilder path = new StringBuilder();

        if (idx != -1) {
            curClass = curClass.substring(idx + packPref.length());

            for (int i = 0, n = curClass.split("\\.").length - 2; i < n; i++)
                path.append("../");
        }

        String[] tokens = tag.text().split("#");

        int lastIdx = tokens[0].lastIndexOf('.');

        String simpleClsName = lastIdx != -1 && lastIdx + 1 < tokens[0].length() ?
            tokens[0].substring(lastIdx + 1) : tokens[0];

        String fullyQClsName = tokens[0].replace(".", "/");

        return "<a href=\"" + path.toString() + fullyQClsName + ".html" +
            (tokens.length > 1 ? ("#" + tokens[1].replace("[]", "...")) : "") +
            "\"><code>" + simpleClsName + (tokens.length > 1 ? ("." + tokens[1]) : "") + "</code></a>";
    }
}