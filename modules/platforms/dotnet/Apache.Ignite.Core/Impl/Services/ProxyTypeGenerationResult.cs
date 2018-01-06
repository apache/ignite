namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Reflection;

    internal class ProxyTypeGenerationResult
    {
        public ProxyTypeGenerationResult(Type type, MethodInfo[] methods)
        {
            Type = type;
            Methods = methods;
        }

        public Type Type { get; private set; }
        public MethodInfo[] Methods { get; private set; }
    }
}