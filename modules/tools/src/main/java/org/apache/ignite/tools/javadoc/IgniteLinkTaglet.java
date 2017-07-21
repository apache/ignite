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
import com.sun.tools.doclets.Taglet;
import java.io.File;
import java.util.Map;

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

    /**
     * @return true since this tag can be used in a field doc comment.
     */
    @Override public boolean inField() {
        return true;
    }

    /**
     * @return true since this tag can be used in a constructor doc comment.
     */
    @Override public boolean inConstructor() {
        return true;
    }

    /**
     * @return true since this tag can be used in a method doc comment.
     */
    @Override public boolean inMethod() {
        return true;
    }

    /**
     * @return true since this tag can be used in an overview doc comment.
     */
    @Override public boolean inOverview() {
        return true;
    }

    /**
     * @return true since this tag can be used in a package doc comment.
     */
    @Override public boolean inPackage() {
        return true;
    }

    /**
     * @return true since this.
     */
    @Override public boolean inType() {
        return true;
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
    @Override public String toString(Tag tag) {
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

    /**
     * This method should not be called since arrays of inline tags do not
     * exist.  Method {@link #toString(Tag)} should be used to convert this
     * inline tag to a string.
     *
     * @param tags the array of <code>Tag</code>s representing of this custom tag.
     */
    @Override public String toString(Tag[] tags) {
        return null;
    }
}