/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.streamer;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Streamer context. Provides access to streamer local store, configured windows and various methods
 * to run streamer queries.
 */
public interface StreamerContext {
    /**
     * Gets instance of dynamic grid projection including all nodes on which this streamer is running.
     *
     * @return Projection with all nodes on which streamer is configured.
     */
    public ClusterGroup projection();

    /**
     * Gets streamer local space. Note that all updates to this space will be local.
     *
     * @return Streamer local space.
     */
    public <K, V> ConcurrentMap<K, V> localSpace();

    /**
     * Gets default event window, i.e. window that is on the first place in streamer configuration.
     *
     * @return Default window.
     */
    public <E> StreamerWindow<E> window();

    /**
     * Gets streamer event window by window name, if no window with such
     * name was configured {@link IllegalArgumentException} will be thrown.
     *
     * @param winName Window name.
     * @return Window instance.
     */
    public <E> StreamerWindow<E> window(String winName);

    /**
     * For context passed to {@link StreamerStage#run(StreamerContext, Collection)} this method will
     * return next stage name in execution pipeline. For context obtained from streamer object, this method will
     * return first stage name.
     *
     * @return Next stage name depending on invocation context.
     */
    public String nextStageName();

    /**
     * Queries all streamer nodes deployed within grid. Given closure will be executed on each node on which streamer
     * is configured. Streamer context local for that node will be passed to closure during execution. All results
     * returned by closure will be added to result collection.
     *
     * @param clo Function to be executed on individual nodes.
     * @return Result received from all streamers.
     * @throws GridException If query execution failed.
     */
    public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo) throws GridException;

    /**
     * Queries streamer nodes deployed within grid. Given closure will be executed on those of passed nodes
     * on which streamer is configured. Streamer context local for that node will be passed to closure during
     * execution. All results returned by closure will be added to result collection.
     *
     * @param clo Function to be executed on individual nodes.
     * @param nodes Optional list of nodes to execute query on, if empty, then all nodes on
     *      which this streamer is running will be queried.
     * @return Result received from all streamers.
     * @throws GridException If query execution failed.
     */
    public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo, Collection<ClusterNode> nodes)
        throws GridException;

    /**
     * Queries all streamer nodes deployed within grid. Given closure will be executed on each streamer node
     * in the grid. No result is collected.
     *
     * @param clo Function to be executed on individual nodes.
     * @throws GridException If closure execution failed.
     */
    public void broadcast(IgniteInClosure<StreamerContext> clo) throws GridException;

    /**
     * Queries streamer nodes deployed within grid. Given closure will be executed on those of passed nodes on
     * which streamer is configured. No result is collected.
     *
     * @param clo Function to be executed on individual nodes.
     * @param nodes Optional list of nodes to execute query on, if empty, then all nodes on
     *      which this streamer is running will be queried.
     * @throws GridException If closure execution failed.
     */
    public void broadcast(IgniteInClosure<StreamerContext> clo, Collection<ClusterNode> nodes) throws GridException;

    /**
     * Queries all streamer nodes deployed within grid. Given closure will be executed on each streamer node in
     * the grid. Streamer context local for that node will be passed to closure during execution. Results returned
     * by closure will be passed to given reducer.
     *
     * @param clo Function to be executed on individual nodes.
     * @param rdc Reducer to reduce results received from remote nodes.
     * @return Reducer result.
     * @throws GridException If query execution failed.
     */
    public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc) throws GridException;

    /**
     * Queries streamer nodes deployed within grid. Given closure will be executed on those of passed nodes on which
     * streamer is configured. Streamer context local for that node will be passed to closure during execution.
     * Results returned by closure will be passed to given reducer.
     *
     * @param clo Function to be executed on individual nodes.
     * @param rdc Reducer to reduce results received from remote nodes.
     * @param nodes Optional list of nodes to execute query on, if empty, then all nodes on
     *      which this streamer is running will be queried.
     * @return Reducer result.
     * @throws GridException If query execution failed.
     */
    public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc,
        Collection<ClusterNode> nodes) throws GridException;
}
