/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.lang;

import java.io.IOException;
import java.util.List;

/**
 * Abstract tree structure printer.
 */
public abstract class GridTreePrinter<T> {
    /**
     * @param rootNode Root tree node.
     * @param a Output.
     */
    public void print(T rootNode, Appendable a) throws IOException {
        printTree(rootNode, "", true, a);
    }

    /**
     * @param rootNode Root tree node.
     * @return String representation of the tree.
     */
    public String print(T rootNode) {
        StringBuilder b = new StringBuilder();

        try {
            print(rootNode, b);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return b.toString();
    }

    /**
     * @param treeNode Tree node.
     * @param prefix Prefix.
     * @param tail Tail child.
     * @param a Output.
     * @throws IOException If failed.
     */
    private void printTree(T treeNode, String prefix, boolean tail, Appendable a) throws IOException {
        List<T> children = getChildren(treeNode);

        int cnt = children == null ? 0 : children.size();

        a.append(prefix).append(tail ? "└── " : "├── ").append(formatTreeNode(treeNode)).append('\n');

        String childPrefix = prefix + (tail ? "    " : "│   ");

        if (children == null)
            a.append(childPrefix).append("└── <list of children is not accessible>\n");
        else {
            for (int i = 0; i < cnt; i++)
                printTree(children.get(i), childPrefix, i == cnt - 1, a);
        }
    }

    /**
     * Returns list of tree node children.
     *
     * @param treeNode The tree node.
     * @return List of children (possibly empty, if it is a leaf page)
     *         or null if the node can't be read (e.g., is locked).
     */
    protected abstract List<T> getChildren(T treeNode);

    /**
     * @param treeNode Tree node.
     * @return String representation.
     */
    protected abstract String formatTreeNode(T treeNode);
}
