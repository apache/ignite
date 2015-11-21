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

namespace Apache.Ignite.Core.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Compute.Extensions;
    using AC = Apache.Ignite.Core.Impl.Common.IgniteArgumentCheck;

    /// <summary>
    /// Extension methods for <see cref="ICompute"/>.
    /// </summary>
    public static class ComputeExtensions
    {
        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Func to execute.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        public static TRes Call<TRes>(this ICompute compute, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.Call(new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Func to execute.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        public static Task<TRes> CallAsync<TRes>(this ICompute compute, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.CallAsync(new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="func">Job to execute.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        public static TRes AffinityCall<TRes>(this ICompute compute, string cacheName, object affinityKey, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.AffinityCall(cacheName, affinityKey, new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="func">Job to execute.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        public static Task<TRes> AffinityCallAsync<TRes>(this ICompute compute, string cacheName, object affinityKey, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.AffinityCallAsync(cacheName, affinityKey, new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduced result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        public static TRes Apply<TArg, TFuncRes, TRes>(this ICompute compute, Func<TArg, TFuncRes> func, 
            ICollection<TArg> args, Func<IList<TFuncRes>, TRes> reduce)
        {
            return Apply(compute, func, args, x => true, reduce);
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduced result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        public static Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(this ICompute compute, Func<TArg, TFuncRes> func, 
            ICollection<TArg> args, Func<IList<TFuncRes>, TRes> reduce)
        {
            return ApplyAsync(compute, func, args, x => true, reduce);
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduced result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="collect">
        /// Collect function: return false to start reducing, return true to continue collecting results.
        /// </param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        public static TRes Apply<TArg, TFuncRes, TRes>(this ICompute compute, Func<TArg, TFuncRes> func, 
            ICollection<TArg> args, Func<TFuncRes, bool> collect, Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");
            AC.NotNull(collect, "collect");
            AC.NotNull(reduce, "reduce");

            return compute.Apply(new ComputeDelegateFunc<TArg, TFuncRes>(func), args,
                new ComputeDelegateReducer<TFuncRes, TRes>(collect, reduce));
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduced result.</typeparam>
        /// <param name="compute">Compute instance.</param>
        /// <param name="func">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="collect">
        /// Collect function: return false to start reducing, return true to continue collecting results.
        /// </param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        public static Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(this ICompute compute, Func<TArg, TFuncRes> func, 
            ICollection<TArg> args, Func<TFuncRes, bool> collect, Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");
            AC.NotNull(collect, "collect");
            AC.NotNull(reduce, "reduce");

            return compute.ApplyAsync(new ComputeDelegateFunc<TArg, TFuncRes>(func), args,
                new ComputeDelegateReducer<TFuncRes, TRes>(collect, reduce));
        }

        /// <summary> 
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for 
        /// every argument in the passed in collection. The number of actual job executions will be 
        /// equal to size of the job arguments collection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to run.</param> 
        /// <param name="args">Job arguments.</param> 
        /// <returns>Сollection of job results.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static ICollection<TRes> Apply<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, 
            ICollection<TArg> args)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.Apply(new ComputeDelegateFunc<TArg, TRes>(func), args);
        }

        /// <summary> 
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for 
        /// every argument in the passed in collection. The number of actual job executions will be 
        /// equal to size of the job arguments collection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to run.</param> 
        /// <param name="args">Job arguments.</param> 
        /// <returns>Сollection of job results.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static Task<ICollection<TRes>> ApplyAsync<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, 
            ICollection<TArg> args)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.ApplyAsync(new ComputeDelegateFunc<TArg, TRes>(func), args);
        }

        /// <summary> 
        /// Executes provided closure job on a node in this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to run.</param> 
        /// <param name="arg">Job argument.</param> 
        /// <returns>Job result for this execution.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static TRes Apply<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, TArg arg)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.Apply(new ComputeDelegateFunc<TArg, TRes>(func), arg);
        }

        /// <summary> 
        /// Executes provided closure job on a node in this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to run.</param> 
        /// <param name="arg">Job argument.</param> 
        /// <returns>Job result for this execution.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static Task<TRes> ApplyAsync<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, TArg arg)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.ApplyAsync(new ComputeDelegateFunc<TArg, TRes>(func), arg);
        }

        /// <summary>
        /// Executes collection of jobs on grid nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="actions">Jobs to execute.</param> 
        public static void Run(this ICompute compute, ICollection<Action> actions)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(actions, "actions");

            var actions0 = new List<IComputeAction>(actions.Count);

            foreach (var action in actions)
                actions0.Add(new ComputeDelegateAction(action));

            compute.Run(actions0);
        }

        /// <summary>
        /// Executes collection of jobs on grid nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="actions">Jobs to execute.</param> 
        public static Task RunAsync(this ICompute compute, ICollection<Action> actions)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(actions, "actions");

            var actions0 = new List<IComputeAction>(actions.Count);

            foreach (var action in actions)
                actions0.Add(new ComputeDelegateAction(action));

            return compute.RunAsync(actions0);
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located 
        /// (this ICompute compute, a.k.a. affinity co-location). 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param> 
        /// <param name="affinityKey">Affinity key.</param> 
        /// <param name="action">Job to execute.</param> 
        public static void AffinityRun(this ICompute compute, string cacheName, object affinityKey, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            compute.AffinityRun(cacheName, affinityKey, new ComputeDelegateAction(action));
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located 
        /// (this ICompute compute, a.k.a. affinity co-location). 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param> 
        /// <param name="affinityKey">Affinity key.</param> 
        /// <param name="action">Job to execute.</param> 
        public static Task AffinityRunAsync(this ICompute compute, string cacheName, object affinityKey, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            return compute.AffinityRunAsync(cacheName, affinityKey, new ComputeDelegateAction(action));
        }

        /// <summary> 
        /// Executes provided job on a node in this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="action">Job to execute.</param> 
        public static void Run(this ICompute compute, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            compute.Run(new ComputeDelegateAction(action));
        }

        /// <summary> 
        /// Executes provided job on a node in this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="action">Job to execute.</param> 
        public static Task RunAsync(this ICompute compute, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            return compute.RunAsync(new ComputeDelegateAction(action));
        }

        /// <summary> 
        /// Broadcasts given job to all nodes in grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="action">Job to broadcast to all projection nodes.</param> 
        public static void Broadcast(this ICompute compute, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            compute.Broadcast(new ComputeDelegateAction(action));
        }

        /// <summary> 
        /// Broadcasts given job to all nodes in grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="action">Job to broadcast to all projection nodes.</param> 
        public static Task BroadcastAsync(this ICompute compute, Action action)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(action, "action");

            return compute.BroadcastAsync(new ComputeDelegateAction(action));
        }

        /// <summary> 
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection. 
        /// Every participating node will return a job result. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to broadcast to all projection nodes.</param> 
        /// <param name="arg">Job closure argument.</param> 
        /// <returns>Collection of results for this execution.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static ICollection<TRes> Broadcast<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, TArg arg)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.Broadcast(new ComputeDelegateFunc<TArg, TRes>(func), arg);
        }

        /// <summary> 
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection. 
        /// Every participating node will return a job result. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to broadcast to all projection nodes.</param> 
        /// <param name="arg">Job closure argument.</param> 
        /// <returns>Collection of results for this execution.</returns> 
        /// <typeparam name="TArg">Type of argument.</typeparam> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static Task<ICollection<TRes>> BroadcastAsync<TArg, TRes>(this ICompute compute, Func<TArg, TRes> func, TArg arg)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.BroadcastAsync(new ComputeDelegateFunc<TArg, TRes>(func), arg);
        }

        /// <summary> 
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.  
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to broadcast to all projection nodes.</param> 
        /// <returns>Collection of results for this execution.</returns> 
        public static ICollection<TRes> Broadcast<TRes>(this ICompute compute, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.Broadcast(new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary> 
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.  
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="func">Job to broadcast to all projection nodes.</param> 
        /// <returns>Collection of results for this execution.</returns> 
        public static Task<ICollection<TRes>> BroadcastAsync<TRes>(this ICompute compute, Func<TRes> func)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(func, "func");

            return compute.BroadcastAsync(new ComputeDelegateFunc<TRes>(func));
        }

        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <returns>Collection of job results for this execution.</returns> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static ICollection<TRes> Call<TRes>(this ICompute compute, ICollection<Func<TRes>> funcs)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");

            var funcs0 = new List<IComputeFunc<TRes>>(funcs.Count);

            foreach (var func in funcs)
                funcs0.Add(new ComputeDelegateFunc<TRes>(func));

            return compute.Call(funcs0);
        }

        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <returns>Collection of job results for this execution.</returns> 
        /// <typeparam name="TRes">Type of job result.</typeparam> 
        public static Task<ICollection<TRes>> CallAsync<TRes>(this ICompute compute, ICollection<Func<TRes>> funcs)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");

            var funcs0 = new List<IComputeFunc<TRes>>(funcs.Count);

            foreach (var func in funcs)
                funcs0.Add(new ComputeDelegateFunc<TRes>(func));

            return compute.CallAsync(funcs0);
        }

        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>Reduced job result for this execution.</returns> 
        /// <typeparam name="TFuncRes">Type of job result.</typeparam> 
        /// <typeparam name="TRes">Type of reduced result.</typeparam> 
        public static TRes Call<TFuncRes, TRes>(this ICompute compute, ICollection<Func<TFuncRes>> funcs, 
            Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");
            AC.NotNull(reduce, "reduce");

            return Call(compute, funcs, x => true, reduce);
        }

        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>Reduced job result for this execution.</returns> 
        /// <typeparam name="TFuncRes">Type of job result.</typeparam> 
        /// <typeparam name="TRes">Type of reduced result.</typeparam> 
        public static Task<TRes> CallAsync<TFuncRes, TRes>(this ICompute compute, ICollection<Func<TFuncRes>> funcs, 
            Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");
            AC.NotNull(reduce, "reduce");

            return CallAsync(compute, funcs, x => true, reduce);
        }

        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <param name="collect">
        /// Collect function: return false to start reducing, return true to continue collecting results.
        /// </param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>Reduced job result for this execution.</returns> 
        /// <typeparam name="TFuncRes">Type of job result.</typeparam> 
        /// <typeparam name="TRes">Type of reduced result.</typeparam> 
        public static TRes Call<TFuncRes, TRes>(this ICompute compute, ICollection<Func<TFuncRes>> funcs, 
            Func<TFuncRes, bool> collect, Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");
            AC.NotNull(collect, "collect");
            AC.NotNull(reduce, "reduce");

            var funcs0 = new List<IComputeFunc<TFuncRes>>(funcs.Count);

            foreach (var func in funcs)
                funcs0.Add(new ComputeDelegateFunc<TFuncRes>(func));

            return compute.Call(funcs0, new ComputeDelegateReducer<TFuncRes, TRes>(collect, reduce));
        }        
        
        /// <summary> 
        /// Executes collection of jobs on nodes within this grid projection. 
        /// <para />
        /// Specified delegate must be Serializable. 
        /// Static functions (includes anonymous functions without captured variables and fields) are serializable.
        /// Non-nested anonymous functions with captured variables are serializable.
        /// Instance functions should have serializable target (owning class).
        /// Anonymous functions which capture fields should have field owning classes serializable.
        /// </summary>
        /// <param name="compute">Compute instance.</param> 
        /// <param name="funcs">Collection of jobs to execute.</param> 
        /// <param name="collect">
        /// Collect function: return false to start reducing, return true to continue collecting results.
        /// </param>
        /// <param name="reduce">Reduce function: combine job results into final result.</param>
        /// <returns>Reduced job result for this execution.</returns> 
        /// <typeparam name="TFuncRes">Type of job result.</typeparam> 
        /// <typeparam name="TRes">Type of reduced result.</typeparam> 
        public static Task<TRes> CallAsync<TFuncRes, TRes>(this ICompute compute, ICollection<Func<TFuncRes>> funcs, 
            Func<TFuncRes, bool> collect, Func<IList<TFuncRes>, TRes> reduce)
        {
            AC.NotNull(compute, "compute");
            AC.NotNull(funcs, "funcs");
            AC.NotNull(collect, "collect");
            AC.NotNull(reduce, "reduce");

            var funcs0 = new List<IComputeFunc<TFuncRes>>(funcs.Count);

            foreach (var func in funcs)
                funcs0.Add(new ComputeDelegateFunc<TFuncRes>(func));

            return compute.CallAsync(funcs0, new ComputeDelegateReducer<TFuncRes, TRes>(collect, reduce));
        }
    }
}
