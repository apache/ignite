package org.apache.ignite.snippets;

import java.util.Collection;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

//tag::node-filter-example[]
public class MyNodeFilter implements IgnitePredicate<ClusterNode> {

    // fill the collection with consistent IDs of the nodes you want to exclude
    private Collection<String> nodesToExclude;

    public MyNodeFilter(Collection<String> nodesToExclude) {
        this.nodesToExclude = nodesToExclude;
    }

    @Override
    public boolean apply(ClusterNode node) {
        return nodesToExclude == null || !nodesToExclude.contains(node.consistentId());
    }
}

//end::node-filter-example[]
