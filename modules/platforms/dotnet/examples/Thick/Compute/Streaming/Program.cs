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

using System.Collections.Concurrent;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cluster;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Resource;
using static System.StringSplitOptions;
using static Apache.Ignite.Examples.Shared.Utils;

Console.WriteLine();
Console.WriteLine(">>> Compute continuous mapper example started.");

using var ignite = Ignition.Start(GetClientNodeConfiguration());

var phraseLength = ignite.GetCompute().Execute(new ContinuousMapperTask(), "Hello Continuous Mapper");

Console.WriteLine();
Console.WriteLine($">>> Total number of characters in the phrase is '{phraseLength}'.");

/// <summary>
/// Demonstrates usage of a continuous mapper. It enables asynchronous continuation of job mappings beyond the
/// completion of the initial <see cref="IComputeTask{TArg,TJobRes,TRes}.Map"/> phase.
/// <para>
/// A string "Hello Continuous Mapper" is provided as an argument for executing the <see cref="ContinuousMapperTask"/>.
/// The task splits the input phrase into distinct words and submits the words for processing.
/// Each participating node processes and prints a single word extracted from the input string, returning the number
/// of characters in that specific word. To illustrate continuous mapping behavior, the next word is mapped to a node
/// only upon receiving the result of the previous word.
/// </para>
/// <para>
/// This example requires an Ignite server node with the <see cref="IgniteConfiguration.PeerAssemblyLoadingMode"/>
/// set to <see cref="Apache.Ignite.Core.Deployment.PeerAssemblyLoadingMode.CurrentAppDomain"/>.
/// You can run another server node in a separate JVM: <c>dotnet run -p ServerNode.csproj</c>
/// </para>
/// <para>
/// Note that <see cref="ComputeTaskNoResultCacheAttribute"/> is optional and instructs Ignite not to store results
/// from individual jobs. In this example we increment the total character count directly within the
/// <see cref="ComputeTaskAdapter{TArg,TJobRes,TTaskRes}.OnResult"/> method, thus eliminating the necessity to
/// aggregate the results during the reduction phase.
/// </para>
/// </summary>
[ComputeTaskNoResultCache]
internal class ContinuousMapperTask : ComputeTaskAdapter<string, int, int>
{
    /// <summary>
    /// This field will be injected via the task continuous mapper.
    /// </summary>
    /// <returns></returns>
    [TaskContinuousMapperResource]
    private readonly IComputeTaskContinuousMapper _mapper = null!;
    
    private readonly ConcurrentQueue<string> _words = new();
    private int _totalCharacterCount;
    
    /// <inheritdoc />
    public override IDictionary<IComputeJob<int>, IClusterNode>? Map(IList<IClusterNode> nodes, string phrase)
    {
        if (string.IsNullOrEmpty(phrase))
        {
            throw new ArgumentException(nameof(phrase));
        }
        
        // Populate the word queue
        var splits = phrase.Split(' ', RemoveEmptyEntries);
        foreach (var word in splits)
        {
            _words.Enqueue(word);
        }
        
        // Send the first word
        SendWord();
        
        // Return null from the Map method since we have sent at least one job
        return null;
    }

    public override ComputeJobResultPolicy OnResult(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
    {
        // Fail-over to another node ff there is an error
        if (res.Exception != null)
        {
            return base.OnResult(res, rcvd);
        }
        
        // Add the result to the total character count
        Interlocked.Add(ref _totalCharacterCount, res.Data);
        
        SendWord();
        
        // Keep waiting if the next word was sent, otherwise work queue is empty and we reduce
        return ComputeJobResultPolicy.Wait;
    }

    /// <inheritdoc />
    public override int Reduce(IList<IComputeJobResult<int>> ignored) => _totalCharacterCount;

    /// <summary>
    /// Sends the next queued word to the next node implicitly selected by the load balancer.
    /// </summary>
    private void SendWord()
    {
        // Remove the first word from the queue
        if (_words.TryDequeue(out var word))
        {
            _mapper.Send(new ContinuousMapperJob(word));
        }
    }

    /// <summary>
    /// The job returns the input word length.
    /// </summary>
    private class ContinuousMapperJob : ComputeJobAdapter<int>
    {
        public ContinuousMapperJob(string word) :  base(word) {}
        
        /// <inheritdoc />
        public override int Execute()
        {
            var word = GetArgument<string>(0);
         
            Console.WriteLine();
            Console.WriteLine($">>> Printing '{word}' from the ignite job at time: {DateTime.Now}");
            
            var length = word.Length;
            
            // Sleep for some time to demonstrate that the jobs are executed sequentially
            Thread.Sleep(TimeSpan.FromSeconds(1));
           
            return length;
        }
    }
}
