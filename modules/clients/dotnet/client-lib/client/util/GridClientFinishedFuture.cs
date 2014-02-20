// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using GridGain.Client;

    /** <summary>Represents a future that already have a result and will never wait.</summary> */
    internal class GridClientFinishedFuture<T> : IGridClientFuture<T> {
        /** <summary>Future execution result.</summary> */
        private readonly Func<T> _get;

        /**
         * <summary>
         * Creates succeeded future with given result.</summary>
         *
         * <param name="get">Future result delegate.</param>
         */
        public GridClientFinishedFuture(Func<T> get) {
            this._get = get;
        }

        /**
         * <summary>
         * Creates succeeded future with given result.</summary>
         *
         * <param name="result">Future result.</param>
         */
        public GridClientFinishedFuture(T result)
            : this(() => result) {
        }

        /**
         * <summary>
         * Synchronously waits for task completion.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public void WaitDone() {
            _get();
        }

        /**
         * <summary>
         * Synchronously waits for task completion.</summary>
         *
         * <param name="timeout">Timeout interval to wait future completes.</param>
         * <returns><c>true</c> if this future completes, <c>false</c> - otherwize.</returns>
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public bool WaitDone(TimeSpan timeout) {
            _get();

            return true;
        }

        /**
         * <summary>
         * Synchronously waits for task completion and returns execution result.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public T Result {
            get {
                return _get();
            }
        }

        /** <summary>Future is done flag.</summary> */
        public bool IsDone {
            get {
                return true;
            }
        }
    }
}
