namespace Apache.Ignite.Core.Binary
{
    using System;

    /// <summary>
    /// Converter for DateTime objects.
    /// </summary>
    public interface IDateTimeConverter
    {
        /// <summary>Convert date to Java ticks.</summary>
        /// <param name="date">Date</param>
        /// <param name="high">High part (milliseconds).</param>
        /// <param name="low">Low part (nanoseconds)</param>
        void ToJavaTicks(DateTime date, out long high, out int low);

        /// <summary>Convert date from Java ticks.</summary>
        /// <param name="high">High part (milliseconds).</param>
        /// <param name="low">Low part (nanoseconds)</param>
        DateTime FromJavaTicks(long high, int low);
    }
}
