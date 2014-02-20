// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    /**
     * <summary>
     * Listener interface for notifying on nodes joining or leaving remote grid.
     * <para/>
     * Since the topology refresh is performed in background, the listeners will
     * not be notified immediately after the node leaves grid, but the maximum time
     * window between remote grid detects node leaving and client receives topology
     * update is <see cref="IGridClientConfiguration.TopologyRefreshFrequency"/>.</summary>
     */
    public interface IGridClientTopologyListener {
        /**
         * <summary>
         * Callback for new nodes joining the remote grid.</summary>
         *
         * <param name="node">New remote node.</param>
         */
        void OnNodeAdded(IGridClientNode node);

        /**
         * <summary>
         * Callback for nodes leaving the remote grid.</summary>
         *
         * <param name="node">Left node.</param>
         */
        void OnNodeRemoved(IGridClientNode node);
    }
}
