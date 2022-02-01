/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>
#include <ignite/cluster/cluster_group.h>

using namespace ignite;
using namespace compute;
using namespace cluster;

/*
 * Function class.
 */
class PrintMsg : public ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<PrintMsg>;
public:
    /*
     * Default constructor.
     */
    PrintMsg() :
        msg("default")
    {
        // No-op.
    }

    /*
     * Constructor.
     *
     * @param text Text.
     */
    PrintMsg(std::string msg) :
        msg(msg)
    {
        // No-op.
    }

    /**
     * Callback.
     * Just print the message.
     *
     */
    virtual void Call()
    {
        std::cout << "# MESSAGE => " <<  msg << std::endl;
    }

private:
    /** Message text. */
    std::string msg;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<PrintMsg>: BinaryTypeDefaultAll<PrintMsg>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Func";
            }

            static void Write(BinaryWriter& writer, const PrintMsg& obj)
            {
                writer.RawWriter().WriteString(obj.msg);
            }

            static void Read(BinaryReader& reader, PrintMsg& dst)
            {
                dst.msg = reader.RawReader().ReadString();
            }
        };
    }
}

int main()
{
    IgniteConfiguration cfgs[4];

    cfgs[0].springCfgPath = "platforms/cpp/examples/cluster-compute-example/config/cluster-compute-example1.xml";
    cfgs[1].springCfgPath = "platforms/cpp/examples/cluster-compute-example/config/cluster-compute-example2.xml";
    cfgs[2].springCfgPath = "platforms/cpp/examples/cluster-compute-example/config/cluster-compute-example2.xml";
    cfgs[3].springCfgPath = "platforms/cpp/examples/cluster-compute-example/config/cluster-compute-example-client.xml";

    try
    {
        // Start server nodes.
        Ignite node1 = Ignition::Start(cfgs[0], "DemoAttributeValue0");
        Ignite node2 = Ignition::Start(cfgs[1], "DemoAttributeValue1I0");
        Ignite node3 = Ignition::Start(cfgs[2], "DemoAttributeValue1I1");

        // Start client node.
        Ignite client = Ignition::Start(cfgs[3], "Client");

        std::cout << std::endl;
        std::cout << ">>> Cluster compute example started." << std::endl;
        std::cout << std::endl;

        // Get binding instances and register our classes as a compute functions.
        node1.GetBinding().RegisterComputeFunc<PrintMsg>();
        node2.GetBinding().RegisterComputeFunc<PrintMsg>();
        node3.GetBinding().RegisterComputeFunc<PrintMsg>();
        client.GetBinding().RegisterComputeFunc<PrintMsg>();

        // Create cluster groups split up by demo attribute value.
        ClusterGroup localGroup = client.GetCluster().AsClusterGroup();
        ClusterGroup group1 = localGroup.ForAttribute("DemoAttribute", "Value0");
        ClusterGroup group2 = localGroup.ForAttribute("DemoAttribute", "Value1");

        // Broadcast compute jobs.
        client.GetCompute(group1).Broadcast(PrintMsg("DemoAttribute=Value0"));
        client.GetCompute(group2).Broadcast(PrintMsg("DemoAttribute=Value1"));

        // Waiting the compute jobs to complete.
        std::cout << std::endl;
        std::cout << ">>> Example finished. Press 'Enter to stop cluster nodes ..." << std::endl;
        std::cout << std::endl;
        std::cin.get();

        // Stop client node first.
        Ignition::Stop(client.GetName(), true);

        // Stop server nodes.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;

        return err.GetCode();
    }

    std::cin.get();

    return 0;
}
