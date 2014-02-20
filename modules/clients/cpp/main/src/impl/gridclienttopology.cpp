// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/gridclienttopology.hpp"

void GridClientTopology::update(const TNodesSet& updatedNodeSet) {
    boost::lock_guard<boost::shared_mutex> lock(mux_);

    nodes_.clear();

    nodes_.insert(updatedNodeSet.begin(), updatedNodeSet.end());
}

void GridClientTopology::remove(const TNodesSet& deletedNodes) {
    boost::lock_guard<boost::shared_mutex> lock(mux_);

    for (auto it = deletedNodes.begin(); it != deletedNodes.end(); ++it)
        nodes_.erase(*it);
}

const TGridClientNodePtr GridClientTopology::node(const GridUuid& uuid) const {
    boost::shared_lock<boost::shared_mutex> lock(mux_);

    for (auto it = nodes_.begin(); it != nodes_.end(); ++it) {
        if (uuid.uuid() == (*it).getNodeId().uuid())
            return TGridClientNodePtr(new GridClientNode(*it));
    }

    return TGridClientNodePtr((GridClientNode *) 0);
}

TNodesSet GridClientTopology::nodes() const {
    boost::shared_lock<boost::shared_mutex> lock(mux_);

    return nodes_;
}

void GridClientTopology::reset() {
    boost::lock_guard<boost::shared_mutex> lock(mux_);

    nodes_.clear();
}
