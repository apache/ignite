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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Set of classes represented as prefix tree.
 * {@code *} symbol is allowed and indicates that all packages and classes are included.
 */
public class ClassSet {
    /** Corresponds to {@code *} symbol. */
    private static final Map<String, Node> ALL = Collections.emptyMap();

    /** Root. */
    private Node root = new Node();

    /**
     * Adds class name to the set.
     *
     * @param clsName Class name.
     */
    public void add(String clsName) {
        String[] tokens = clsName.split("\\.");

        Node cur = root;

        for (int i = 0; i < tokens.length; i++) {
            if (cur.children == ALL)
                return;

            if (tokens[i].equals("*")) {
                if (i != tokens.length - 1)
                    throw new IllegalArgumentException("Incorrect class name format.");

                cur.children = ALL;

                return;
            }

            if (cur.children == null)
                cur.children = new HashMap<>();

            Node n = cur.children.get(tokens[i]);

            if (n == null) {
                n = new Node();

                cur.children.put(tokens[i], n);
            }

            cur = n;
        }
    }

    /**
     * @param clsName Class name.
     */
    public boolean contains(String clsName) {
        String[] tokens = clsName.split("\\.");

        Node cur = root;

        for (int i = 0; i < tokens.length; i++) {
            if (cur.children == ALL)
                return true;

            if (cur.children == null)
                return false;

            Node n = cur.children.get(tokens[i]);

            if (n == null)
                return false;

            if (i == tokens.length - 1)
                return true;

            cur = n;
        }

        return false;
    }

    /** */
    private static class Node {
        /** Children. */
        private Map<String, Node> children;
    }
}
