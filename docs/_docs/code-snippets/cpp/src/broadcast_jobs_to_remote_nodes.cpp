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
//tag::remote-nodes[]
class PrintNodeIdAction : public ComputeFunc<void> {
public:
    virtual void Call() {
        std::cout << "Hello node " <<  Ignition::Get().GetCluster().GetLocalNode().GetId() << std::endl;
    }
};
namespace ignite { namespace binary {
    template<> struct BinaryType<PrintNodeIdAction>: BinaryTypeDefaultAll<PrintNodeIdAction> {
        static void GetTypeName(std::string& dst) {
            dst = "PrintNodeIdAction";
        }
        static void Write(BinaryWriter& writer, const PrintNodeIdAction& obj) {}
        static void Read(BinaryReader& reader, PrintNodeIdAction& dst) {}
    };
}}
void void RemotesBroadcastDemo()
{
    Ignite ignite = Ignition::Get();
    IgniteCluster cluster = ignite.GetCluster();
    // Get compute instance which will only execute
    // over remote nodes, i.e. all the nodes except for this one.
    Compute compute = ignite.GetCompute(cluster.AsClusterGroup().ForRemotes());
    // Broadcast to all remote nodes and print the ID of the node
    // on which this closure is executing.
    compute.Broadcast(PrintNodeIdAction());
}
//end::remote-nodes[]