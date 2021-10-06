namespace Apache.Ignite.Core.Services
{
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Service proxy context builder.
    /// </summary>
    public class ServiceProxyContextBuilder
    {
        /** Context attributes. */
        private readonly Hashtable _values;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceProxyContextBuilder()
        {
            _values = new Hashtable();
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Context attribute name.</param>
        /// <param name="value">Context attribute value.</param>
        public ServiceProxyContextBuilder(string name, object value)
        {
            _values = new Hashtable {{name, value}};
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="values">Context attributes.</param>
        public ServiceProxyContextBuilder(Dictionary<string, object> values)
        {
            _values = new Hashtable(values);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Context attribute name.</param>
        /// <param name="value">Context attribute value.</param>
        public ServiceProxyContextBuilder Add(string name, object value)
        {
            _values.Add(name, value);

            return this;
        }

        /// <summary>
        /// Create an instance of service proxy context.
        /// </summary>
        /// <returns>Service proxy context.</returns>
        internal ServiceProxyContext Build()
        {
            return new ServiceProxyContextImpl(_values);
        }
    }
}