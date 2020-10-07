using System;
using Apache.Ignite.Core;

namespace dotnet_helloworld
{
    public static class BaselineTopology
    {
        public static void Activate()
        {
            // tag::activate[]
            IIgnite ignite = Ignition.Start();
            ignite.GetCluster().SetActive(true);
            // end::activate[]
        }

        public static void EnableAutoAdjust()
        {
            // tag::enable-autoadjustment[]
            IIgnite ignite = Ignition.Start();
            ignite.GetCluster().SetBaselineAutoAdjustEnabledFlag(true);
            ignite.GetCluster().SetBaselineAutoAdjustTimeout(30000);
            // end::enable-autoadjustment[]
        }

        public static void DisableAutoAdjust()
        {
            IIgnite ignite = Ignition.Start();
            // tag::disable-autoadjustment[]
            ignite.GetCluster().SetBaselineAutoAdjustEnabledFlag(false);
            // end::disable-autoadjustment[]
        }
    }
}
