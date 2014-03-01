/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTTOPOLOGYLISTENER_HPP_INCLUDED
#define GRIDCLIENTTOPOLOGYLISTENER_HPP_INCLUDED

#include <gridgain/gridconf.hpp>

class GridClientNode;

/**
 * Listener interface for notifying on nodes joining or leaving remote grid.
 * <p>
 * Since the topology refresh is performed in background, the listeners will not be notified
 * immediately after the node leaves grid. The maximum time window between the remote grid detects
 * topology change and client receives topology update is {@link GridClientConfiguration#getTopologyRefreshFrequency()}.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientTopologyListener {
public:
    /** Virtual destructor. */
    virtual ~GridClientTopologyListener() {}
    /**
     * Callback for new nodes joining the remote grid.
     *
     * @param node New remote node.
     */
    virtual void onNodeAdded(const GridClientNode& node) = 0;

    /**
     * Callback for nodes leaving the remote grid.
     *
     * @param node Left node.
     */
    virtual void onNodeRemoved(const GridClientNode& node) = 0;
};

#endif // GRIDCLIENTTOPOLOGYLISTENER_HPP_INCLUDED
