namespace Apache.Ignite.Linq
{
    using System;

    public interface IUpdateDescriptor<T>
    {
        IUpdateDescriptor<T> Set<TProp>(Func<T, TProp> selector, TProp value);
        IUpdateDescriptor<T> Set<TProp>(Func<T, TProp> selector, Func<T, TProp> valueBuilder);
    }
}