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
        
        /** Keep binary flag. */
        private readonly bool _keepBinary;

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
        public ClientContinuousQueryHandle(ClientSocket socket, bool keepBinary, long continuousQueryId, long? initialQueryId)
        {
            _socket = socket;
            _keepBinary = keepBinary;
            _continuousQueryId = continuousQueryId;
            _initialQueryId = initialQueryId;
        }

        /** <inheritdoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> GetInitialQueryCursor()
        {
            if (_initialQueryId == null)
            {
                // Should not happen since user gets IContinuousQueryHandle in this case.
                throw new InvalidOperationException("Continuous query does not have initial query.");
            }
            
            return new ClientQueryCursor<TK, TV>(
                _socket, _initialQueryId.Value, _keepBinary, null, ClientOp.QueryScanCursorGetPage);
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

                _socket.RemoveNotificationHandler(_continuousQueryId);

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
