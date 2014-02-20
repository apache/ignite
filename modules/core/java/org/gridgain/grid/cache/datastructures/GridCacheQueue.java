// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This interface provides a rich API for working with distributed queues based on In-Memory Data Grid.
 * <p>
 * <h1 class="header">Overview</h1>
 * Cache queue provides an access to cache elements using typical queue API. Cache queue also implements
 * {@link Collection} interface and provides all methods from collections including
 * {@link Collection#addAll(Collection)}, {@link Collection#removeAll(Collection)}, and
 * {@link Collection#retainAll(Collection)} methods for bulk operations. Note that all
 * {@link Collection} methods in the queue may throw {@link GridRuntimeException} in case
 * of failure. If you prefer to catch checked exceptions, use methods that end with
 * {@code 'x'}, such as {@link #addx(Object)} for example, which throw {@link GridException}.
 * <p>
 * All queue operations have synchronous and asynchronous counterparts.
 * <h1 class="header">Bounded vs Unbounded</h1>
 * Queues can be {@code unbounded} or {@code bounded}. {@code Bounded} queues can
 * have maximum capacity. Queue capacity can be set at creation time and cannot be
 * changed later. Here is an example of how to create {@code bounded} {@code LIFO} queue with
 * capacity of {@code 1000} items.
 * <pre name="code" class="java">
 * GridCacheQueue&lt;String&gt; queue = cache().queue("anyName", LIFO, 1000);
 * ...
 * queue.add("item");
 * </pre>
 * For {@code bounded} queues <b>blocking</b> operations, such as {@link #take()} or {@link #put(Object)}
 * are available. These operations will block until queue capacity changes to make the operation
 * possible.
 * <h1 class="header">Queue Types</h1>
 * The following types of queues are available in GridGain:
 * <ul>
 *      <li>{@link GridCacheQueueType#FIFO} (default)</li>
 *      <li>{@link GridCacheQueueType#LIFO}</li>
 *      <li>{@link GridCacheQueueType#PRIORITY}</li>
 * </ul>
 * For more information about queue types refer to {@link GridCacheQueueType} documentation.
 * <p>
 * {@code Priority} queues allow for sorting queue items by priority. Priority in the queue item can
 * be set using annotation {@link GridCacheQueuePriority}. Only one field or method can be annotated
 * with priority in queue item class. Annotated fields or methods must be of {@code int} or {@code Integer}
 * types. Here is an example of how annotate queue item:
 * <pre name="code" class="java">
 * private static class PriorityMethodItem {
 *       // Priority field.
 *       private final int priority;
 *
 *       private PriorityMethodItem(int priority) {
 *           this.priority = priority;
 *       }
 *
 *       // Annotated priority method.
 *       &#64;GridCacheQueuePriority
 *       int priority() {
 *           return priority;
 *       }
 *   }
 * </pre>
 * <h1 class="header">Collocated vs Non-collocated</h1>
 * Queue items can be placed on one node or distributed throughout grid nodes
 * (governed by {@code collocated} parameter). {@code Non-collocated} mode is provided only
 * for partitioned caches. If {@code collocated} parameter is {@code true}, then all queue items
 * will be collocated on one node, otherwise items will be distributed through all grid nodes.
 * Unless explicitly specified, by default queues are {@code collocated}.
 * <p>
 * Here is an example of how create {@code unbounded} {@link GridCacheQueueType#PRIORITY} queue
 * in non-collocated mode.
 * <pre name="code" class="java">
 * GridCacheQueue&lt;String&gt; queue = cache().queue("anyName", PRIORITY, 0 &#047;*unbounded*&#047;, false &#047;*non-collocated*&#047;);
 * ...
 * queue.add("item");
 * </pre>
 * <h1 class="header">Creating Cache Queues</h1>
 * Instances of distributed cache queues can be created by calling the following method
 * on {@link GridCacheDataStructures} API:
 * <ul>
 *     <li>{@link GridCacheDataStructures#queue(String, GridCacheQueueType, int, boolean, boolean)}</li>
 * </ul>
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheDataStructures#queue(String, GridCacheQueueType, int, boolean, boolean)
 * @see GridCacheDataStructures#removeQueue(String)
 * @see GridCacheDataStructures#removeQueue(String, int)
 */
public interface GridCacheQueue<T> extends GridMetadataAware, Collection<T> {
    /**
     * Gets queue name.
     *
     * @return Queue name.
     */
    public String name();

    /**
     * Gets queue type {@link GridCacheQueueType}.
     *
     * @return Queue type.
     */
    public GridCacheQueueType type();

    /**
     * Adds specified item to the queue without blocking. If queue is {@code bounded} and full, then
     * item will not be added and {@code false} will be returned.
     * <p>
     * If operation fails then {@link GridRuntimeException} is thrown.
     *
     * @param item Item to add.
     * @return {@code True} if item was added, {@code false} if item wasn't added because queue is full.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean add(T item) throws GridRuntimeException;

    /**
     * Adds specified item to the queue without blocking. If queue is {@code bounded} and full, then
     * item will not be added and {@code false} will be returned.
     * <p>
     * Unlike {@link #add(Object)}, this method throws {@link GridException} if operation fails.
     *
     * @param item Queue item to add.
     * @return {@code True} if item was added, {@code false} if item wasn't added because queue is full.
     * @throws GridException If operation failed.
     */
    public boolean addx(T item) throws GridException;

    /**
     * Asynchronously adds specified item to the queue. If queue is {@code bounded} and full, then
     * item will not be added and {@code false} will be returned from the future.
     * <p>
     * If operation fails then {@link GridException} is thrown.
     *
     * @param item Item to add.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> addAsync(T item);

    /**
     * Bulk operation for adding more than one item to queue at once without blocking. Only one internal
     * transaction will be created (as opposed to multiple ones if method {@link #add(Object)} was called
     * multiple times).
     * <p>
     * If queue is {@code bounded} and does not have enough capacity to add all items, then none of the
     * items will be added and {@code false} will be returned.
     *
     * @param items Items to add.
     * @return {@code True} if items were added, {@code false} if queue did not have enough capacity to
     *      fit all the items.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean addAll(Collection<? extends T> items) throws GridRuntimeException;

    /**
     * Bulk operation for adding more than one item to queue at once without blocking. Only one internal
     * transaction will be created (as opposed to multiple ones if method {@link #add(Object)} was called
     * multiple times).
     * <p>
     * If queue is {@code bounded} and does not have enough capacity to add all items, then none of the
     * items will be added and {@code false} will be returned.
     * <p>
     * Unlike {@link #addAll(Collection)}, this method throws {@link GridException} if operation fails.
     *
     * @param items Items to add.
     * @return {@code True} if items were added, {@code false} if queue did not have enough capacity to
     *      fit all the items.
     * @throws GridException If operation failed.
     */
    public boolean addAllx(Collection<? extends T> items) throws GridException;

    /**
     * Asynchronous bulk operation for adding more than one item to queue at once. Only one internal
     * transaction will be created (as opposed to multiple ones if method {@link #add(Object)} was called
     * multiple times).
     * <p>
     * If queue is {@code bounded} and does not have enough capacity to add all items, then none of the
     * items will be added and {@code false} will be returned from the future.
     *
     * @param items Items to add.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> addAllAsync(Collection<? extends T> items);

    /**
     * Returns {@code true} if this queue contains the specified element.
     *
     * @param item Element whose presence in this queue is to be tested.
     * @return {@code true} If this queue contains the specified
     *      element.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean contains(Object item) throws GridRuntimeException;

    /**
     * Returns {@code true} if this queue contains the specified element.
     * <p>
     * Unlike {@link #contains(Object)}, this method throws {@link GridException}
     * if operation fails.
     *
     * @param item Element whose presence in this queue is to be tested.
     * @return {@code True} if this queue contains the specified
     *      element.
     * @throws GridException If operation failed.
     */
    public boolean containsx(Object item) throws GridException;

    /**
     * Returns {@code true} if this queue contains all of the elements
     * in the specified collection.
     *
     * @param  items Collection to be checked for containment in this queue.
     * @return {@code True} if this queue contains all of the elements
     *      in the specified collection.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean containsAll(Collection<?> items) throws GridRuntimeException;

    /**
     * Returns {@code true} if this queue contains all of the elements
     * in the specified collection.
     * <p>
     * Unlike {@link #containsAll(Collection)}, this method throws {@link GridException}
     * if operation fails.
     *
     * @param  items Collection to be checked for containment in this queue.
     * @return {@code True} if this queue contains all of the elements
     *      in the specified collection.
     * @throws GridException If operation failed.
     */
    public boolean containsAllx(Collection<?> items) throws GridException;

    /**
     * Removes all of the elements from this queue.
     *
     * @throws GridRuntimeException if operation failed.
     */
    @Override public void clear() throws GridRuntimeException;

    /**
     * Removes all of the elements from this queue.
     *
     * @throws GridException If operation failed.
     */
    public void clearx() throws GridException;

    /**
     * Removes all of the elements from this queue. Method is used in massive queues with huge numbers of elements.
     *
     * @param batchSize Batch size.
     * @throws GridRuntimeException if operation failed.
     */
    public void clear(int batchSize) throws GridRuntimeException;

    /**
     * Removes all of the elements from this queue. Method is used in massive queues with huge numbers of elements.
     *
     * @param batchSize Batch size.
     * @throws GridException If operation failed.
     */
    public void clearx(int batchSize) throws GridException;

    /**
     * Removes a single instance of the specified element from this
     * queue, if it is present.
     *
     * @param item Element to be removed from this queue, if present.
     * @return {@code True} if an element was removed as a result of this call.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean remove(Object item) throws GridRuntimeException;

    /**
     * Removes a single instance of the specified element from this
     * queue, if it is present.
     * <p>
     * Unlike {@link #remove(Object)}, this method throws {@link GridException}
     * if operation fails.
     *
     * @param item Item to delete.
     * @return {@code True} if an element was removed as a result of this call.
     * @throws GridException If operation failed.
     */
    public boolean removex(T item) throws GridException;

    /**
     * Asynchronously removes a single instance of the specified element from this
     * queue, if it is present.
     *
     * @param item Item to delete.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> removeAsync(T item);

    /**
     * Bulk operation that removes all of this queue's elements that are also
     * contained in the specified collection.  After this call returns,
     * this queue will contain no elements in common with the specified
     * collection.
     *
     * @param items collection containing elements to be removed from this queue.
     * @return {@code True} if this queue changed as a result of the call.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean removeAll(Collection<?> items) throws GridRuntimeException;

    /**
     * Bulk operation that removes all of this queue's elements that are also
     * contained in the specified collection. After this call returns,
     * this queue will contain no elements in common with the specified
     * collection.
     * <p>
     * Unlike {@link #removeAll(Collection)} this method throws {@link GridException}
     * if operation fails.
     *
     * @param items collection containing elements to be removed from this queue.
     * @return {@code True} if this queue changed as a result of the call.
     * @throws GridException If operation failed.
     */
    public boolean removeAllx(Collection<?> items) throws GridException;

    /**
     * Asynchronous bulk operation that removes all of this queue's elements that are also
     * contained in the specified collection. After this call returns,
     * this queue will contain no elements in common with the specified
     * collection.
     *
     * @param items collection containing elements to be removed from this queue.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> removeAllAsync(Collection<?> items);

    /**
     * Returns {@code true} if this queue contains no elements.
     *
     * @return {@code True} if this queue contains no elements.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean isEmpty() throws GridRuntimeException;

    /**
     * Returns {@code true} if this queue contains no elements.
     * <p>
     * Unlike {@link #isEmpty()} this method throws {@link GridException}
     * if operation fails.
     *
     * @return {@code True} if this queue contains no elements.
     * @throws GridException If operation failed.
     */
    public boolean isEmptyx() throws GridException;

    /**
     * Returns an iterator over the elements in this queue.
     *
     * @return Iterator over the elements in this queue.
     */
    @Override public Iterator<T> iterator() throws GridRuntimeException;

    /**
     * Returns an array containing all of the elements in this queue.
     *
     * @return An array containing all of the elements in this queue.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public Object[] toArray() throws GridRuntimeException;

    /**
     * Returns an array containing all of the elements in this queue;
     * the runtime type of the returned array is that of the specified array.
     * If the queue fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of the
     * specified array and the size of this queue.
     *
     * @param a The array into which the elements of this queue are to be
     *      stored, if it is big enough; otherwise, a new array of the same
     *      runtime type is allocated for this purpose.
     * @return An array containing all of the elements in this queue.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public <T> T[] toArray(T[] a) throws GridRuntimeException;

    /**
     * Retains only the elements in this queue that are contained in the
     * specified collection.  In other words, removes from
     * this collection all of its elements that are not contained in the
     * specified collection.
     *
     * @param items Collection containing elements to be retained in this collection.
     * @return {@code True} if this collection changed as a result of the call.
     * @throws GridRuntimeException If operation failed.
     */
    @Override public boolean retainAll(Collection<?> items) throws GridRuntimeException;

    /**
     * Retains only the elements in this queue that are contained in the
     * specified collection.  In other words, removes from
     * this collection all of its elements that are not contained in the
     * specified collection.
     *
     * @param items Collection containing elements to be retained in this collection.
     * @return {@code True} if this collection changed as a result of the call.
     * @throws GridException If operation failed.
     */
    public boolean retainAllx(Collection<?> items) throws GridException;

    /**
     * Retains only the elements in this queue that are contained in the
     * specified collection.  In other words, removes from
     * this collection all of its elements that are not contained in the
     * specified collection.
     *
     * @param items Collection containing elements to be retained in this collection.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> retainAllAsync(Collection<?> items);

    /**
     * Returns the number of elements in this queue. If this queue
     * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of elements in this collection
     * @throws GridRuntimeException If operation failed.
     */
    @Override public int size() throws GridRuntimeException;

    /**
     * Retrieves and removes the head item of the queue, or returns {@code null} if this queue is empty.
     *
     * @return Item from the head of the queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T poll() throws GridException;

    /**
     * Asynchronously retrieves and removes the head item of the queue.
     * Future returns {@code null} if this queue is empty.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> pollAsync();

    /**
     * Retrieves and removes the tail item of the queue, or returns {@code null} if this queue is empty.
     *
     * @return Item from the tail of the queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T pollLast() throws GridException;

    /**
     * Retrieves and removes the tail item of the queue.
     * Future returns {@code null} if this queue is empty.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> pollLastAsync();

    /**
     * Retrieves, but does not remove, the head of this queue, or returns {@code null} if this queue is empty.
     *
     * @return The head of this queue, or {@code null} if this queue is empty.
     * @throws GridException If operation failed.
     */
    @Nullable public T peek() throws GridException;

    /**
     * Asynchronously retrieves, but does not remove, the head of this queue.
     * Future returns {@code null} if this queue is empty.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> peekAsync();

    /**
     * Retrieves, but does not remove, the tail of this queue, or returns {@code null} if this queue is empty.
     *
     * @return Future for the operation.
     * @throws GridException If operation failed.
     */
    @Nullable public T peekLast() throws GridException;

    /**
     * Asynchronously retrieves, but does not remove, the tail of this queue.
     * Future returns {@code null} if this queue is empty.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> peekLastAsync();

    /**
     * Gets position of the specified item in the queue. First element (head of the queue) is at position {@code 0}.
     * <p>
     * Note this operation is supported only in {@code collocated} mode.
     *
     * @param item Item to get position for.
     * @return Position of specified item in the queue or {@code -1} if item is not found.
     * @throws GridException If operation failed or operations executes in {@code non-collocated} mode.
     */
    public int position(T item) throws GridException;

    /**
     * Gets items from the queue at specified positions. First element (head of the queue) is at position {@code 0}.
     * <p>
     * Note this operation is supported only in {@code collocated} mode.
     *
     * @param positions Positions of items to get from queue.
     * @return Queue items at specified positions.
     * @throws GridException If operation failed or operations executes in {@code non-collocated} mode.
     */
    @Nullable public Collection<T> items(Integer... positions) throws GridException;

    /**
     * Puts specified item to the queue. Waits until place will be available in this queue.
     *
     * @param item Queue item to put.
     * @throws GridException If operation failed.
     */
    public void put(T item) throws GridException;

    /**
     * Try to put specified item to the queue during timeout.
     *
     * @param item Queue item to put.
     * @param timeout Timeout.
     * @param unit Type of time representations.
     * @return {@code false} if timed out while waiting for queue to go below maximum capacity,
     *      {@code true} otherwise. If queue is not bounded, then {@code true} is always returned.
     * @throws GridException If operation failed.
     */
    public boolean put(T item, long timeout, TimeUnit unit) throws GridException;

    /**
     * Puts specified item to the queue asynchronously.
     *
     * @param item Queue item to put.
     * @return Future for the operation.
     */
    public GridFuture<Boolean> putAsync(T item);

    /**
     * Retrieves and removes the head item of the queue. Waits if no elements are present in this queue.
     *
     * @return Item from the head of the queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T take() throws GridException;

    /**
     * Retrieves and removes the tail item of the queue. Waits if no elements are present in this queue.
     *
     * @return Item from the tail of the queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T takeLast() throws GridException;

    /**
     * Try to retrieve and remove the head item of the queue during timeout.
     *
     * @param timeout Timeout.
     * @param unit Type of time representations.
     * @return Item from the head of the queue, or {@code null} if method timed out
     *      before queue had at least one item.
     * @throws GridException If operation failed or timeout was exceeded.
     */
    @Nullable public T take(long timeout, TimeUnit unit) throws GridException;

    /**
     * Try to retrieve and remove the tail item of the queue during timeout.
     *
     * @param timeout Timeout.
     * @param unit Type of time representations.
     * @return Item from the tail of the queue, or {@code null} if method timed out
     *      before queue had at least one item.
     * @throws GridException If operation failed or timeout was exceeded.
     */
    @Nullable public T takeLast(long timeout, TimeUnit unit) throws GridException;

    /**
     * Retrieves and removes the head item of the queue asynchronously.
     *
     * @return Future for the take operation.
     */
    public GridFuture<T> takeAsync();

    /**
     * Try to retrieve and remove the tail item of the queue asynchronously.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> takeLastAsync();

    /**
     * Retrieves, but does not remove, the head of this queue. Waits if no elements are present in this queue.
     *
     * @return The head of this queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T get() throws GridException;

    /**
     * Retrieves, but does not remove, the tail of this queue. Waits if no elements are present in this queue.
     *
     * @return The tail of this queue.
     * @throws GridException If operation failed.
     */
    @Nullable public T getLast() throws GridException;

    /**
     * Try to retrieve but does not remove the head of this queue within given timeout.
     * Waits if no elements are present in this queue.
     *
     * @param timeout Timeout.
     * @param unit Type of time representations.
     * @return The head of this queue or {@code null} if method timed out
     *      before queue had at least one item.
     * @throws GridException If operation failed or timeout was exceeded.
     */
    @Nullable public T get(long timeout, TimeUnit unit) throws GridException;

    /**
     * Try to retrieve but does not remove the tail of this queue within given timeout.
     * Waits if no elements are present in this queue.
     *
     * @param timeout Timeout.
     * @param unit Type of time representations.
     * @return The tail of this queue, or {@code null} if method timed out
     *      before queue had at least one item.
     * @throws GridException If operation failed or timeout was exceeded.
     */
    @Nullable public T getLast(long timeout, TimeUnit unit) throws GridException;

    /**
     * Try to retrieve but does not remove the head of this queue asynchronously.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> getAsync();

    /**
     * Try to retrieve but does not remove the tail of this queue asynchronously.
     *
     * @return Future for the operation.
     */
    public GridFuture<T> getLastAsync();

    /**
     * Clears the queue asynchronously.
     *
     * @return Future for the operation.
     */
    public GridFuture<Boolean> clearAsync();

    /**
     * Gets size of the queue.
     *
     * @return Size of the queue.
     * @throws GridException If operation failed.
     */
    public int sizex() throws GridException;

    /**
     * Gets maximum number of elements of the queue.
     *
     * @return Maximum number of elements. If queue is unbounded {@code Integer.MAX_SIZE} will return.
     * @throws GridException If operation failed.
     */
    public int capacity() throws GridException;

    /**
     * Returns {@code true} if this queue is bounded.
     *
     * @return {@code true} if this queue is bounded.
     * @throws GridException If operation failed.
     */
    public boolean bounded() throws GridException;

    /**
     * Returns {@code true} if this queue can be kept on the one node only.
     * Returns {@code false} if this queue can be kept on the many nodes.
     *
     * @return {@code true} if this queue is in {@code collocated} mode {@code false} otherwise.
     * @throws GridException If operation failed.
     */
    public boolean collocated() throws GridException;

    /**
     * Gets status of queue.
     *
     * @return {@code true} if queue was removed from cache {@code false} otherwise.
     */
    public boolean removed();
}
