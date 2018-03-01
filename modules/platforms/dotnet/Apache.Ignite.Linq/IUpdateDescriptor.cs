namespace Apache.Ignite.Linq
{
    using System;
    using Apache.Ignite.Core.Cache;

    public interface IUpdateDescriptor<out TKey, out TValue>
    {
        IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<ICacheEntry<TKey, TValue>, TProp> selector, TProp value);
        IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<ICacheEntry<TKey, TValue>, TProp> selector, Func<ICacheEntry<TKey,TValue>, TProp> valueBuilder);
    }

    public class UD<TKey, TValue> : IUpdateDescriptor<TKey, TValue>
    {
        public UD(ICacheEntry<TKey,TValue> cacheEntry)
        {
            
        }

        public IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<ICacheEntry<TKey, TValue>, TProp> selector, TProp value)
        {
            return this;
        }

        public IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<ICacheEntry<TKey, TValue>, TProp> selector, Func<ICacheEntry<TKey, TValue>, TProp> valueBuilder)
        {
            return this;
        }
    }

    public static class UD
    {
        public static IUpdateDescriptor<TKey, TValue> Create<TKey, TValue>(ICacheEntry<TKey, TValue> entry)
        {
            return new UD<TKey, TValue>(entry);
        }
    }
}