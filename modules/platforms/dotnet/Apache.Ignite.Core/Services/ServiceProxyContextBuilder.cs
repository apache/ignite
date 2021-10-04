namespace Apache.Ignite.Core.Services
{
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Services;

    public class ServiceProxyContextBuilder
    {
        private readonly Hashtable _values;

        public ServiceProxyContextBuilder()
        {
            _values = new Hashtable();
        }
        
        public ServiceProxyContextBuilder(Dictionary<string, object> values)
        {
            _values = new Hashtable(values);
        }
        
        public ServiceProxyContextBuilder(string name, object value)
        {
            _values = new Hashtable {{name, value}};
        }

        public ServiceProxyContextBuilder Add(string name, object value)
        {
            _values.Add(name, value);

            return this;
        }

        internal ServiceProxyContext Build()
        {
            return new ServiceProxyContextImpl(_values);
        }
    }
}