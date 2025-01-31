namespace Apache.Ignite.Core.Impl.Cache.Platform
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// No-op implementation of platform cache.
    /// </summary>
    internal sealed class NoOpPlatformCache : IPlatformCache
    {
        /** <inheritdoc /> */
        public bool IsStopped { get; private set; }

        /** <inheritdoc /> */
        public void Update(IBinaryStream stream, Marshaller marshaller)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void UpdateFromThreadLocal(int partition, AffinityTopologyVersion affinityTopologyVersion)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void Stop()
        {
            IsStopped = true;
        }

        /** <inheritdoc /> */
        public bool TryGetValue<TKey, TVal>(TKey key, out TVal val)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public int GetSize(int? partition)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw new System.NotImplementedException();
        }
        
        /** <inheritdoc /> */
        public void SetThreadLocalPair<TK, TV>(TK key, TV val)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void ResetThreadLocalPair()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetEntries<TK, TV>(int? partition = null)
        {
            throw new System.NotImplementedException();
        }
    }
}