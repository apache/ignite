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

package org.apache.ignite.configuration.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;

/** */
public interface ConfigurationUtil {
    /** */
    static String escape(String key) {
        return key.replaceAll("([.\\\\])", "\\\\$1");
    }

    /** */
    static String unescape(String key) {
        return key.replaceAll("\\\\([.\\\\])", "$1");
    }

    /** */
    static List<String> split(String keys) {
        String[] split = keys.split("(?<!\\\\)[.]", -1);

        for (int i = 0; i < split.length; i++)
            split[i] = unescape(split[i]);

        return Arrays.asList(split);
    }

    /** */
    static String join(List<String> keys) {
        return keys.stream().map(ConfigurationUtil::escape).collect(Collectors.joining("."));
    }

    /**
     * Search for the configuration node by the list of keys.
     *
     * @param keys Random access list with keys.
     * @param node Node where method will search for subnode.
     * @return Either {@link TraversableTreeNode} or {@link Serializable} depending on the keys and schema.
     * @throws KeyNotFoundException If node is not found.
     */
    static Object find(List<String> keys, TraversableTreeNode node) throws KeyNotFoundException {
        assert keys instanceof RandomAccess : keys.getClass();

        var visitor = new ConfigurationVisitor() {
            /** */
            private int i;

            /** */
            private Object res;

            @Override public void visitLeafNode(String key, Serializable val) {
                if (i != keys.size())
                    throw new KeyNotFoundException("Configuration value '" + join(keys.subList(0, i)) + "' is a leaf");

                res = val;
            }

            @Override public void visitInnerNode(String key, InnerNode node) {
                if (i == keys.size())
                    res = node;
                else if (node == null)
                    throw new KeyNotFoundException("Configuration node '" + join(keys.subList(0, i)) + "' is null");
                else {
                    try {
                        node.traverseChild(keys.get(i++), this);
                    }
                    catch (NoSuchElementException e) {
                        throw new KeyNotFoundException("Configuration '" + join(keys.subList(0, i)) + "' is not found");
                    }
                }
            }

            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                if (i == keys.size())
                    res = node;
                else {
                    String name = keys.get(i++);

                    visitInnerNode(name, node.get(name));
                }
            }
        };

        node.accept(null, visitor);

        return visitor.res;
    }
}
