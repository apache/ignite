# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from contract_diff import compare_sources
from view_sources import VIEW_RULE_SOURCES, view_companions


ROOT = "modules/core/src/main/java/org/apache/ignite/"
CACHES = ROOT + "spi/systemview/view/CacheView.java"
REGISTRY = ROOT + "internal/processors/cache/ClusterCachesInfo.java"
IMPORTS = """import org.apache.ignite.internal.systemview.Order;
import org.apache.ignite.internal.systemview.SystemViewDescriptor;
"""
ROW = ("package org.apache.ignite.spi.systemview.view;\n" + IMPORTS + """
@SystemViewDescriptor public class CacheView {
    @Order public String cacheName() { return ""; }
    @Order(1) public int cacheId() { return 0; }
    public int cacheSize() { return 0; }
}""")
REGISTER = """package org.apache.ignite.internal.processors.cache;
import org.apache.ignite.internal.systemview.CacheViewWalker;
public class ClusterCachesInfo {
    static final String CACHES_VIEW = "caches";
    void start() { ctx.systemView().registerView(CACHES_VIEW, "Caches", new CacheViewWalker(), rows, factory); }
}"""


def findings(before, after):
    sources = {(side, path): data for side, files in (("before", before), ("after", after)) for path, data in files.items()}
    outcomes = compare_sources([(p, p) for p in before.keys() & after.keys()], sources)
    return [(p, text) for p, facts in outcomes.items() for text in facts if "column order" in text]


class SystemViewColumnsTest(unittest.TestCase):
    def test_effective_order_in_a_registered_view(self):
        """Moving cacheId ahead of cacheName reports its old and new positions in the registered view."""
        old = {CACHES: ROW, REGISTRY: REGISTER}
        new = {**old, CACHES: ROW.replace("@Order(1)", "@Order(value = -1)")}
        self.assertEqual(findings(old, new), [(CACHES,
            "Changed column order in system view caches: cacheId moved from position 2 to 1")])

    def test_equivalent_order_and_uncertain_ranks(self):
        """Renumbering priorities, appending a column or losing annotation resolution produces no order finding."""
        old = {CACHES: ROW, REGISTRY: REGISTER}
        edits = (ROW.replace("@Order public", "@Order(value = 0) public").replace("@Order(1)", "@Order(3)"),
                 ROW.replace("public int cacheSize()", "public int lastColumn() { return 0; } public int cacheSize()"),
                 ROW.replace("@Order(1)", "@Order(PRIORITY)"), ROW.replace("@Order(1)", "@Order"),
                 ROW.replace("systemview.Order;", "systemview.*;"),
                 ROW.replace("public int cacheSize()", "private int helper() { return 1; } public int cacheSize()"))
        for source in edits:
            with self.subTest(source=source):
                self.assertEqual(findings(old, {**old, CACHES: source}), [])

    def test_inserting_or_removing_an_earlier_column_shifts_existing_positions(self):
        """Adding a leading column or removing it changes the index clients use for cacheName."""
        old = {CACHES: ROW, REGISTRY: REGISTER}
        inserted = ROW.replace("@Order public", "@Order(-1) public int newColumn() { return 1; } @Order public")
        new = {**old, CACHES: inserted}
        self.assertEqual(findings(old, new), [(CACHES,
            "Changed column order in system view caches: cacheName moved from position 1 to 2")])
        self.assertEqual(findings(new, old), [(CACHES,
            "Changed column order in system view caches: cacheName moved from position 2 to 1")])

    def test_only_walkers_registered_in_both_revisions(self):
        """Changing a descriptor while replacing its generated walker leaves its order to review."""
        old = {CACHES: ROW, REGISTRY: REGISTER}
        new = {CACHES: ROW.replace("@Order(1)", "@Order(-1)"), REGISTRY: REGISTER.replace("new CacheViewWalker()", "custom")}
        self.assertEqual(findings(old, new), [])
        shadowed = REGISTER.replace("void start()", "void start(String CACHES_VIEW)")
        self.assertEqual(findings({**old, REGISTRY: shadowed}, {**new, REGISTRY: shadowed}), [])

    def test_inherited_columns_follow_the_registered_subclass(self):
        """A shared parent changes, so the subclass's registered view reports the parent source file."""
        parent = ROOT + "spi/systemview/view/datastructures/AbstractDataStructureView.java"
        child = parent.replace("AbstractDataStructure", "AtomicLong")
        registry = ROOT + "internal/processors/datastructures/DataStructuresProcessor.java"
        package = "package org.apache.ignite.spi.systemview.view.datastructures;\n"
        base = package + IMPORTS + """@SystemViewDescriptor abstract class AbstractDataStructureView<T> {
            @Order public String name() { return ""; }
            @Order(10) public int groupId() { return 0; }
        }"""
        row = package + IMPORTS + """public class AtomicLongView extends AbstractDataStructureView<Long> {
            @Order(1) public long value() { return 0; }
        }"""
        reg = REGISTER.replace("processors.cache", "processors.datastructures").replace("ClusterCachesInfo", "DataStructuresProcessor")
        reg = reg.replace("CacheViewWalker", "AtomicLongViewWalker").replace('"caches"', '"ds.atomiclongs"')
        old = {parent: base, child: row, registry: reg}
        self.assertEqual(findings(old, {**old, parent: base.replace("@Order(10)", "@Order(-1)")}), [
            (parent, "Changed column order in system view ds.atomiclongs: groupId moved from position 3 to 1")])
        self.assertTrue({registry, child, parent} <= view_companions({parent}))

    def test_only_row_changes_load_companions(self):
        """A row edit needs its registration. A registry-only or generator edit needs no extra reads."""
        self.assertEqual(view_companions({CACHES}), {CACHES, REGISTRY})
        self.assertEqual(view_companions({REGISTRY}), set())
        self.assertEqual(view_companions({CACHES, *VIEW_RULE_SOURCES}), set())

    def test_scan_queries_object_exposes_the_generated_walker(self):
        """The registered scan view object forwards its name and walker to AbstractSystemView."""
        row = CACHES.replace("CacheView", "ScanQueryView")
        registry = REGISTRY.replace("ClusterCachesInfo", "GridCacheSharedContext")
        wrapper = ROOT + "internal/managers/systemview/ScanQuerySystemView.java"
        base = wrapper.replace("ScanQuerySystemView", "AbstractSystemView")
        reg = """package org.apache.ignite.internal.processors.cache;
            import org.apache.ignite.internal.managers.systemview.ScanQuerySystemView;
            class GridCacheSharedContext {
                void start() { ctx.systemView().registerView(new ScanQuerySystemView<>(caches)); }
            }"""
        holder = """package org.apache.ignite.internal.managers.systemview;
            import org.apache.ignite.internal.systemview.ScanQueryViewWalker;
            class ScanQuerySystemView<T> extends AbstractSystemView<ScanQueryView> {
                ScanQuerySystemView(T caches) { super("scan.queries", "Scan queries", new ScanQueryViewWalker()); }
            }"""
        old = {row: ROW.replace("CacheView", "ScanQueryView"), registry: reg, wrapper: holder}
        new = {**old, row: old[row].replace("@Order(1)", "@Order(-1)")}
        self.assertEqual(view_companions({row}), set(old))
        self.assertEqual(findings(old, new), [(row,
            "Changed column order in system view scan.queries: cacheId moved from position 2 to 1")])
        dynamic = holder.replace("new ScanQueryViewWalker()", "chooseWalker()")
        override = holder.replace("ScanQuerySystemView(T", "public Walker walker() { return other; } ScanQuerySystemView(T")
        for file, source in ((wrapper, dynamic), (wrapper, override), (registry, reg.replace("(caches)", "(caches) {}"))):
            with self.subTest(source=source):
                self.assertEqual(findings(old, {**new, file: source}), [])
        self.assertNotIn(wrapper, view_companions({row, base}))
        self.assertEqual(findings(old, {**new, base: "package org.apache.ignite.internal.managers.systemview; class AbstractSystemView {}"}), [])
