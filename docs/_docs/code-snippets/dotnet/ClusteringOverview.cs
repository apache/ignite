using Apache.Ignite.Core;
using Apache.Ignite.Core.Communication.Tcp;

namespace dotnet_helloworld
{
    class ClusteringOverview
    {
        static void Foo()
        {
            // tag::ClientsAndServers[]
            Ignition.ClientMode = true;
            Ignition.Start();
            // end::ClientsAndServers[]

             // tag::CommunicationSPI[]
             var cfg = new IgniteConfiguration
             {
                 CommunicationSpi = new TcpCommunicationSpi
                 {
                     LocalPort = 1234
                 }
             };
            Ignition.Start(cfg);
            // end::CommunicationSPI[]
        }

        static void ClientCfg()
        {
            // tag::ClientCfg[]
            var cfg = new IgniteConfiguration
            {
                // Enable client mode.
                ClientMode = true
            };
            
            // Start Ignite in client mode.
            Ignition.Start(cfg);
            // end::ClientCfg[]
        }
    }
}
