/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

import java.util.List;

/**
 * Tree node info. It is used to represent tree nodes in recursive traversal.
 */
class TreeNode {
    /** */
    final long pageId;

    /** */
    final PageIO io;

    /** */
    final String additionalInfo;

    /** */
    final List<TreeNode> children;

    /** */
    public TreeNode(long pageId, PageIO io, String additionalInfo, List<TreeNode> children) {
        this.pageId = pageId;
        this.io = io;
        this.additionalInfo = additionalInfo;
        this.children = children;
    }
}