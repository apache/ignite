namespace Apache.Ignite.Linq
{
    using System;
    using Apache.Ignite.Core.Cache;

    public interface IUpdateDescriptor<T, TV>
    {
        IUpdateDescriptor<T, TV> Set<TProp>(Func<TV, TProp> selector, TProp value);
        IUpdateDescriptor<T, TV> Set<TProp>(Func<TV, TProp> selector, Func<ICacheEntry<T,TV>, TProp> valueBuilder);
    }
}