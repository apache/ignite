/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Defines a distributed session for particular task execution.
 * <h1 class="header">Description</h1>
 * This interface defines a distributed session that exists for particular task
 * execution. Task session is distributed across the parent task and all grid
 * jobs spawned by it, so attributes set on a task or on a job can be viewed on
 * other jobs. Correspondingly attributes set on any of the jobs can also be
 * viewed on a task.
 * <p>
 * Session has 2 main features: {@code attribute} and {@code checkpoint}
 * management. Both attributes and checkpoints can be used from task itself and
 * from the jobs belonging to this task. Session attributes and checkpoints can
 * be set from any task or job methods. Session attribute and checkpoint consistency
 * is fault tolerant and is preserved whenever a job gets failed over to
 * another node for execution. Whenever task execution ends, all checkpoints
 * saved within session with {@link GridComputeTaskSessionScope#SESSION_SCOPE} scope
 * will be removed from checkpoint storage. Checkpoints saved with
 * {@link GridComputeTaskSessionScope#GLOBAL_SCOPE} will outlive the session and
 * can be viewed by other tasks.
 * <p>
 * The sequence in which session attributes are set is consistent across
 * the task and all job siblings within it. There will never be a
 * case when one job sees attribute A before attribute B, and another job sees
 * attribute B before A. Attribute order is identical across all session
 * participants. Attribute order is also fault tolerant and is preserved
 * whenever a job gets failed over to another node.
 * <p>
 * <h1 class="header">Connected Tasks</h1>
 * Note that apart from setting and getting session attributes, tasks or
 * jobs can choose to wait for a certain attribute to be set using any of
 * the {@code waitForAttribute(...)} methods. Tasks and jobs can also
 * receive asynchronous notifications about a certain attribute being set
 * through {@link GridComputeTaskSessionAttributeListener} listener. Such feature
 * allows grid jobs and tasks remain <u><i>connected</i></u> in order
 * to synchronize their execution with each other and opens a solution for a
 * whole new range of problems.
 * <p>
 * Imagine for example that you need to compress a very large file (let's say
 * terabytes in size). To do that in grid environment you would split such
 * file into multiple sections and assign every section to a remote job for
 * execution. Every job would have to scan its section to look for repetition
 * patterns. Once this scan is done by all jobs in parallel, jobs would need to
 * synchronize their results with their siblings so compression would happen
 * consistently across the whole file. This can be achieved by setting
 * repetition patterns discovered by every job into the session. Once all
 * patterns are synchronized, all jobs can proceed with compressing their
 * designated file sections in parallel, taking into account repetition patterns
 * found by all the jobs in the split. Grid task would then reduce (aggregate)
 * all compressed sections into one compressed file. Without session attribute
 * synchronization step this problem would be much harder to solve.
 * <p>
 * <h1 class="header">Session Injection</h1>
 * Session can be injected into a task or a job using IoC (dependency
 * injection) by attaching {@link GridTaskSessionResource @GridTaskSessionResource}
 * annotation to a field or a setter method inside of {@link GridComputeTask} or
 * {@link ComputeJob} implementations as follows:
 * <pre name="code" class="java">
 * ...
 * // This field will be injected with distributed task session.
 * &#64GridTaskSessionResource
 * private GridComputeTaskSession ses;
 * ...
 * </pre>
 * or from a setter method:
 * <pre name="code" class="java">
 * // This setter method will be automatically called by the system
 * // to set grid task session.
 * &#64GridTaskSessionResource
 * void setSession(GridComputeTaskSession ses) {
 *     this.ses = ses;
 * }
 * </pre>
 * <h1 class="header">Example</h1>
 * To see example on how to use {@code GridComputeTaskSession} refer to
 * <a href="https://github.com/gridgain/gridgain/tree/master/examples/java/org/gridgain/examples/helloworld/gridify/session" target="github">HelloWorld Distributed Task Session Example</a>
 * on GitHub.
 */
public interface GridComputeTaskSession {
    /**
     * Gets task name of the task this session belongs to.
     *
     * @return Task name of the task this session belongs to.
     */
    public String getTaskName();

    /**
     * Gets ID of the node on which task execution originated.
     *
     * @return ID of the node on which task execution originated.
     */
    public UUID getTaskNodeId();

    /**
     * Gets start of computation time for the task.
     *
     * @return Start of computation time for the task.
     */
    public long getStartTime();

    /**
     * Gets end of computation time for the task. No job within the task
     * will be allowed to execute passed this time.
     *
     * @return End of computation time for the task.
     */
    public long getEndTime();

    /**
     * Gets session ID of the task being executed.
     *
     * @return Session ID of the task being executed.
     */
    public IgniteUuid getId();

    /**
     * Gets class loader responsible for loading all classes within task.
     * <p>
     * Note that for classes that were loaded remotely from other nodes methods
     * {@link Class#getResource(String)} or {@link ClassLoader#getResource(String)}
     * will always return {@code null}. Use
     * {@link Class#getResourceAsStream(String)} or {@link ClassLoader#getResourceAsStream(String)}
     * instead.
     *
     * @return Class loader responsible for loading all classes within task.
     */
    public ClassLoader getClassLoader();

    /**
     * Gets a collection of all grid job siblings. Job siblings are grid jobs
     * that are executing within the same task.
     * <p>
     * If task uses continuous mapper (i.e. it injected into task class) then
     * job siblings will be requested from task node for each apply.
     *
     * @return Collection of grid job siblings executing within this task.
     * @throws GridException If job siblings can not be received from task node.
     */
    public Collection<ComputeJobSibling> getJobSiblings() throws GridException;

    /**
     * Refreshes collection of job siblings. This method has no effect when invoked
     * on originating node, as the list of siblings is always most recent. However,
     * when using <tt>continuous mapping</tt> (see {@link GridComputeTaskContinuousMapper}),
     * list of siblings on remote node may not be fresh. In that case, this method
     * will re-request list of siblings from originating node.
     *
     * @return Refreshed collection of job siblings.
     * @throws GridException If refresh failed.
     */
    public Collection<ComputeJobSibling> refreshJobSiblings() throws GridException;

    /**
     * Gets job sibling for a given ID.
     * <p>
     * If task uses continuous mapper (i.e. it injected into task class) then
     * job sibling will be requested from task node for each apply.
     *
     *
     * @param jobId Job ID to get the sibling for.
     * @return Grid job sibling for a given ID.
     * @throws GridException If job sibling can not be received from task node.
     */
    @Nullable public ComputeJobSibling getJobSibling(IgniteUuid jobId) throws GridException;

    /**
     * Sets session attributed. Note that task session is distributed and
     * this attribute will be propagated to all other jobs within this task and task
     * itself - i.e., to all accessors of this session.
     * Other jobs then will be notified by {@link GridComputeTaskSessionAttributeListener}
     * callback than an attribute has changed.
     * <p>
     * This method is no-op if the session has finished.
     *
     * @param key Attribute key.
     * @param val Attribute value. Can be {@code null}.
     * @throws GridException If sending of attribute message failed.
     */
    public void setAttribute(Object key, @Nullable Object val) throws GridException;

    /**
     * Gets an attribute set by {@link #setAttribute(Object, Object)} or {@link #setAttributes(Map)}
     * method. Note that this attribute could have been set by another job on
     * another node.
     * <p>
     * This method is no-op if the session has finished.
     *
     * @param key Attribute key.
     * @param <K> Attribute key type.
     * @param <V> Attribute value type.
     * @return Gets task attribute for given name.
     */
    @Nullable public <K, V> V getAttribute(K key);

    /**
     * Sets task attributes. This method exists so one distributed replication
     * operation will take place for the whole group of attributes passed in.
     * Use it for performance reasons, rather than {@link #setAttribute(Object, Object)}
     * method, whenever you need to set multiple attributes.
     * <p>
     * This method is no-op if the session has finished.
     *
     * @param attrs Attributes to set.
     * @throws GridException If sending of attribute message failed.
     */
    public void setAttributes(Map<?, ?> attrs) throws GridException;

    /**
     * Gets all attributes.
     *
     * @return All session attributes.
     */
    public Map<?, ?> getAttributes();

    /**
     * Add listener for the session attributes.
     *
     * @param lsnr Listener to add.
     * @param rewind {@code true} value will result in calling given listener for all
     *      already received attributes, while {@code false} value will result only
     *      in new attribute notification. Settings {@code rewind} to {@code true}
     *      allows for a simple mechanism that prevents the loss of notifications for
     *      the attributes that were previously received or received while this method
     *      was executing.
     */
    public void addAttributeListener(GridComputeTaskSessionAttributeListener lsnr, boolean rewind);

    /**
     * Removes given listener.
     *
     * @param lsnr Listener to remove.
     * @return {@code true} if listener was removed, {@code false} otherwise.
     */
    public boolean removeAttributeListener(GridComputeTaskSessionAttributeListener lsnr);

    /**
     * Waits for the specified attribute to be set. If this attribute is already in session
     * this method will return immediately.
     *
     * @param key Attribute key to wait for.
     * @param timeout Timeout in milliseconds to wait for. {@code 0} means indefinite wait.
     * @param <K> Attribute key type.
     * @param <V> Attribute value type.
     * @return Value of newly set attribute.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    public <K, V> V waitForAttribute(K key, long timeout) throws InterruptedException;

    /**
     * Waits for the specified attribute to be set or updated with given value. Note that
     * this method will block even if attribute is set for as long as its value is not equal
     * to the specified.
     *
     * @param key Attribute key to wait for.
     * @param val Attribute value to wait for. Can be {@code null}.
     * @param timeout Timeout in milliseconds to wait for. {@code 0} means indefinite wait.
     * @param <K> Attribute key type.
     * @param <V> Attribute value type.
     * @return Whether or not specified key/value pair has been set.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    public <K, V> boolean waitForAttribute(K key, @Nullable V val, long timeout) throws InterruptedException;

    /**
     * Waits for the specified attributes to be set. If these attributes are already in session
     * this method will return immediately.
     *
     * @param keys Attribute keys to wait for.
     * @param timeout Timeout in milliseconds to wait for. {@code 0} means indefinite wait.
     * @return Attribute values mapped by their keys.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) throws InterruptedException;

    /**
     * Waits for the specified attributes to be set or updated with given values. Note that
     * this method will block even if attributes are set for as long as their values are not equal
     * to the specified.
     *
     * @param attrs Key/value pairs to wait for.
     * @param timeout Timeout in milliseconds to wait for. {@code 0} means indefinite wait.
     * @return Whether or not key/value pair has been set.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException;

    /**
     * Saves intermediate state of a job or task to a storage. The storage implementation is defined
     * by {@link GridCheckpointSpi} implementation used.
     * <p>
     * Long running jobs may decide to store intermediate state to protect themselves from failures.
     * This way whenever a job fails over to another node, it can load its previously saved state via
     * {@link #loadCheckpoint(String)} method and continue with execution.
     * <p>
     * This method defaults checkpoint scope to {@link GridComputeTaskSessionScope#SESSION_SCOPE} and
     * implementation will automatically remove the checkpoint at the end of the session. It is
     * analogous to calling {@link #saveCheckpoint(String, Object, GridComputeTaskSessionScope, long)
     * saveCheckpoint(String, Serializable, GridCheckpointScope.SESSION_SCOPE, 0}.
     *
     * @param key Key to be used to load this checkpoint in future.
     * @param state Intermediate job state to save.
     * @throws GridException If failed to save intermediate job state.
     * @see #loadCheckpoint(String)
     * @see #removeCheckpoint(String)
     * @see GridCheckpointSpi
     */
    public void saveCheckpoint(String key, Object state) throws GridException;

    /**
     * Saves intermediate state of a job to a storage. The storage implementation is defined
     * by {@link GridCheckpointSpi} implementation used.
     * <p>
     * Long running jobs may decide to store intermediate state to protect themselves from failures.
     * This way whenever a job fails over to another node, it can load its previously saved state via
     * {@link #loadCheckpoint(String)} method and continue with execution.
     * <p>
     * The life time of the checkpoint is determined by its timeout and scope.
     * If {@link GridComputeTaskSessionScope#GLOBAL_SCOPE} is used, the checkpoint will outlive
     * its session, and can only be removed by calling {@link GridCheckpointSpi#removeCheckpoint(String)}
     * from {@link org.apache.ignite.Ignite} or another task or job.
     *
     * @param key Key to be used to load this checkpoint in future.
     * @param state Intermediate job state to save.
     * @param scope Checkpoint scope. If equal to {@link GridComputeTaskSessionScope#SESSION_SCOPE}, then
     *      state will automatically be removed at the end of task execution. Otherwise, if scope is
     *      {@link GridComputeTaskSessionScope#GLOBAL_SCOPE} then state will outlive its session and can be
     *      removed by calling {@link #removeCheckpoint(String)} from another task or whenever
     *      timeout expires.
     * @param timeout Maximum time this state should be kept by the underlying storage. Value {@code 0} means that
     *       timeout will never expire.
     * @throws GridException If failed to save intermediate job state.
     * @see #loadCheckpoint(String)
     * @see #removeCheckpoint(String)
     * @see GridCheckpointSpi
     */
    public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout)
        throws GridException;

    /**
     * Saves intermediate state of a job or task to a storage. The storage implementation is defined
     * by {@link GridCheckpointSpi} implementation used.
     * <p>
     * Long running jobs may decide to store intermediate state to protect themselves from failures.
     * This way whenever a job fails over to another node, it can load its previously saved state via
     * {@link #loadCheckpoint(String)} method and continue with execution.
     * <p>
     * The life time of the checkpoint is determined by its timeout and scope.
     * If {@link GridComputeTaskSessionScope#GLOBAL_SCOPE} is used, the checkpoint will outlive
     * its session, and can only be removed by calling {@link GridCheckpointSpi#removeCheckpoint(String)}
     * from {@link org.apache.ignite.Ignite} or another task or job.
     *
     * @param key Key to be used to load this checkpoint in future.
     * @param state Intermediate job state to save.
     * @param scope Checkpoint scope. If equal to {@link GridComputeTaskSessionScope#SESSION_SCOPE}, then
     *      state will automatically be removed at the end of task execution. Otherwise, if scope is
     *      {@link GridComputeTaskSessionScope#GLOBAL_SCOPE} then state will outlive its session and can be
     *      removed by calling {@link #removeCheckpoint(String)} from another task or whenever
     *      timeout expires.
     * @param timeout Maximum time this state should be kept by the underlying storage. Value <tt>0</tt> means that
     *      timeout will never expire.
     * @param overwrite Whether or not overwrite checkpoint if it already exists.
     * @throws GridException If failed to save intermediate job state.
     * @see #loadCheckpoint(String)
     * @see #removeCheckpoint(String)
     * @see GridCheckpointSpi
     */
    public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout,
        boolean overwrite) throws GridException;

    /**
     * Loads job's state previously saved via {@link #saveCheckpoint(String, Object, GridComputeTaskSessionScope, long)}
     * method from an underlying storage for a given {@code key}. If state was not previously
     * saved, then {@code null} will be returned. The storage implementation is defined by
     * {@link GridCheckpointSpi} implementation used.
     * <p>
     * Long running jobs may decide to store intermediate state to protect themselves from failures.
     * This way whenever a job starts, it can load its previously saved state and continue
     * with execution.
     *
     * @param key Key for intermediate job state to load.
     * @param <T> Type of the checkpoint state.
     * @return Previously saved state or {@code null} if no state was found for a given {@code key}.
     * @throws GridException If failed to load job state.
     * @see #removeCheckpoint(String)
     * @see GridCheckpointSpi
     */
    @Nullable public <T> T loadCheckpoint(String key) throws GridException;

    /**
     * Removes previously saved job's state for a given {@code key} from an underlying storage.
     * The storage implementation is defined by {@link GridCheckpointSpi} implementation used.
     * <p>
     * Long running jobs may decide to store intermediate state to protect themselves from failures.
     * This way whenever a job starts, it can load its previously saved state and continue
     * with execution.
     *
     * @param key Key for intermediate job state to load.
     * @return {@code true} if job state was removed, {@code false} if state was not found.
     * @throws GridException If failed to remove job state.
     * @see #loadCheckpoint(String)
     * @see GridCheckpointSpi
     */
    public boolean removeCheckpoint(String key) throws GridException;

    /**
     * Gets a collection of grid nodes IDs.
     *
     * @return Collection of grid nodes IDs for the task's split.
     */
    public Collection<UUID> getTopology();

    /**
     * Gets future that will be completed when task "<tt>map</tt>" step has completed
     * (which means that {@link GridComputeTask#map(List, Object)} method has finished).
     *
     * @return Future that will be completed when task "<tt>map</tt>" step has completed.
     */
    public IgniteFuture<?> mapFuture();
}
