using Apache.Ignite.Core;

namespace dotnet_helloworld
{
    public static class IgniteLifecycle
    {
        public static void Start()
        {
            // tag::start[]
            var cfg = new IgniteConfiguration();
            IIgnite ignite = Ignition.Start(cfg);
            // end::start[]
        }

        public static void StartClient()
        {
            // tag::start-client[]
            var cfg = new IgniteConfiguration
            {
                ClientMode = true
            };
            IIgnite ignite = Ignition.Start(cfg);
            // end::start-client[]
        }

        public static void StartDispose()
        {
            // tag::disposable[]
            var cfg = new IgniteConfiguration();
            using (IIgnite ignite = Ignition.Start(cfg))
            {
                //
            }
            // end::disposable[]
        }
    }
}
