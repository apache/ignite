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
 * Content of the deserialized page. When content is gained, we can free the page buffer.
 */
class PageContent {
    /** */
    final PageIO io;

    /** List of children page ids, or links to root pages (for meta leaf). */
    final List<Long> linkedPageIds;

    /** List of items (for leaf pages). */
    final List<Object> items;

    /** Some info. */
    final String info;

    /** */
    public PageContent(PageIO io, List<Long> linkedPageIds, List<Object> items, String info) {
        this.io = io;
        this.linkedPageIds = linkedPageIds;
        this.items = items;
        this.info = info;
    }
}