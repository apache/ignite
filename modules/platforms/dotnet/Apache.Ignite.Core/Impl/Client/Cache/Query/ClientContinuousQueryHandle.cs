namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;

    internal class ClientContinuousQueryHandle<TK, TV> : IContinuousQueryHandle<ICacheEntry<TK, TV>>
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Cursor ID. */
        private readonly long _continuousQueryId;

        /** Initial query ID. */
        private readonly long? _initialQueryId;

        /** */
        private readonly object _disposeSyncRoot = new object();

        /** */
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientContinuousQueryHandle{TK, TV}"/>.
        /// </summary>
        public ClientContinuousQueryHandle(ClientSocket socket, long continuousQueryId, long? initialQueryId)
        {
            _socket = socket;
            _continuousQueryId = continuousQueryId;
            _initialQueryId = initialQueryId;
        }

        public IQueryCursor<ICacheEntry<TK, TV>> GetInitialQueryCursor()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose; <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                // TODO: How do we dispose of initial query? It should have it's own Dispose method.
                _socket.DoOutInOp<object>(ClientOp.ResourceClose,
                    ctx => ctx.Writer.WriteLong(_continuousQueryId), null);

                _disposed = true;
            }
        }

        /** <inheritdoc /> */
        ~ClientContinuousQueryHandle()
        {
            Dispose(false);
        }
    }
}
