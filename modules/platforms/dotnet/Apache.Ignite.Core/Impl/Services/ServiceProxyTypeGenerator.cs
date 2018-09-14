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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Reflection.Emit;
    using Apache.Ignite.Core.Impl.Binary;
    using ProxyAction = System.Func<System.Reflection.MethodBase, object[], object>;

    /// <summary>
    /// Emits service proxy type.
    /// </summary>
    internal static class ServiceProxyTypeGenerator
    {
        /** */
        private static readonly Type ActionType = typeof(ProxyAction);

        /** */
        private static readonly MethodInfo InvokeMethod = ActionType.GetMethod("Invoke");

        /** */
        private static readonly MethodInfo Finalizer = typeof(object)
            .GetMethod("Finalize", BindingFlags.Instance | BindingFlags.NonPublic);

        /** Classic .NET Method. */
        private static readonly MethodInfo AppDomainDefineAssembly = typeof(AppDomain).GetMethod(
            "DefineDynamicAssembly",
            BindingFlags.Public | BindingFlags.Instance,
            null,
            new[] {typeof(AssemblyName), typeof(AssemblyBuilderAccess)},
            null);

        /** .NET Core Method. */
        private static readonly MethodInfo AssemblyBuilderDefineAssembly = typeof(AssemblyBuilder).GetMethod(
            "DefineDynamicAssembly",
            BindingFlags.Public | BindingFlags.Static,
            null,
            new[] {typeof(AssemblyName), typeof(AssemblyBuilderAccess)},
            null);

        /** */
        private static readonly ModuleBuilder ModuleBuilder = CreateModuleBuilder();

        /// <summary>
        /// Generates the proxy for specified service type.
        /// </summary>
        public static Tuple<Type, MethodInfo[]> Generate(Type serviceType)
        {
            Debug.Assert(serviceType != null);
            Debug.Assert(serviceType.FullName != null);

            var isClass = serviceType.IsClass;
            var proxyType = ModuleBuilder.DefineType(serviceType.FullName,
                TypeAttributes.Class, isClass ? serviceType : null);

            var buildContext = new ProxyBuildContext(proxyType, serviceType);
            if (!isClass)
            {
                proxyType.AddInterfaceImplementation(serviceType);
            }

            GenerateFields(buildContext);
            GenerateStaticConstructor(buildContext);
            GenerateConstructor(buildContext);

            Debug.Assert(Finalizer != null);

            buildContext.Methods = ReflectionUtils.GetMethods(buildContext.ServiceType)
                .Where(m => m.IsVirtual && m != Finalizer)
                .ToArray();

            for (var i = 0; i < buildContext.Methods.Length; i++)
            {
                GenerateMethod(buildContext, i);
            }

            var type = proxyType.CreateType();
            return Tuple.Create(type, buildContext.Methods);
        }

        /// <summary>
        /// Creates a builder for a temporary module.
        /// </summary>
        private static ModuleBuilder CreateModuleBuilder()
        {
            var name = Guid.NewGuid().ToString("N");

            var asmName = Expression.Constant(new AssemblyName(name));
            var access = Expression.Constant(AssemblyBuilderAccess.RunAndCollect);
            var domain = Expression.Constant(AppDomain.CurrentDomain);

            // AppDomain.DefineDynamicAssembly is not available on .NET Core;
            // AssemblyBuilder.DefineDynamicAssembly is not available on .NET 4.
            // Both of them can not be called with Reflection.
            // So we have to be creative and use expression trees.
            var callExpr = AppDomainDefineAssembly != null
                ? Expression.Call(domain, AppDomainDefineAssembly, asmName, access)
                : Expression.Call(AssemblyBuilderDefineAssembly, asmName, access);

            var callExprLambda = Expression.Lambda<Func<AssemblyBuilder>>(callExpr);

            var assemblyBuilder = callExprLambda.Compile()();

            return assemblyBuilder.DefineDynamicModule(name);
        }

        /// <summary>
        /// Generates readonly fields: action and method array.
        /// </summary>
        private static void GenerateFields(ProxyBuildContext buildContext)
        {
            // Static field - empty object array to optimize calls without parameters.
            buildContext.EmptyParametersField = buildContext.ProxyType.DefineField("_emptyParameters",
                typeof(object[]), FieldAttributes.Static | FieldAttributes.Private | FieldAttributes.InitOnly);

            // Instance field for function to invoke.
            buildContext.ActionField = buildContext.ProxyType.DefineField("_action", ActionType,
                FieldAttributes.Private | FieldAttributes.InitOnly);

            // Field - array with methods of service's type.
            buildContext.MethodsField = buildContext.ProxyType.DefineField("_methods", typeof(MethodInfo[]),
                FieldAttributes.Private | FieldAttributes.InitOnly);
        }

        /// <summary>
        /// Generates the static constructor (type initializer).
        /// </summary>
        private static void GenerateStaticConstructor(ProxyBuildContext buildContext)
        {
            var cb = buildContext.ProxyType.DefineConstructor(
                MethodAttributes.Static | MethodAttributes.Private | MethodAttributes.HideBySig,
                CallingConventions.Standard, new Type[0]);
            var gen = cb.GetILGenerator();
            //fill _emptyParameters field
            gen.Emit(OpCodes.Ldc_I4_0);
            gen.Emit(OpCodes.Newarr, typeof(object));
            gen.Emit(OpCodes.Stsfld, buildContext.EmptyParametersField);

            gen.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Generates the constructor which calls base class (when necessary) and initializes fields.
        /// </summary>
        private static void GenerateConstructor(ProxyBuildContext buildContext)
        {
            var baseType = buildContext.ServiceType;
            var isClass = baseType.IsClass;

            ConstructorInfo baseCtr = null;
            if (isClass)
            {
                baseCtr = baseType.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                    null, new Type[0], null);
                if (baseCtr == null)
                    throw new NotSupportedException(
                        "Service proxy does not support base types without parameterless constructor: " +
                        baseType.FullName);
            }

            var cb = buildContext.ProxyType.DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis,
                new[] {ActionType, typeof(MethodInfo[])});
            var gen = cb.GetILGenerator();

            if (isClass)
            {
                // Load "this".
                gen.Emit(OpCodes.Ldarg_0);
                // Call base constructor.
                gen.Emit(OpCodes.Call, baseCtr);
            }

            // Assign parameters to fields.
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldarg_1);
            gen.Emit(OpCodes.Stfld, buildContext.ActionField);

            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldarg_2);
            gen.Emit(OpCodes.Stfld, buildContext.MethodsField);

            gen.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Generates the overriding method which delegates to ProxyAction.
        /// </summary>
        private static void GenerateMethod(ProxyBuildContext buildContext, int methodIndex)
        {
            var method = buildContext.Methods[methodIndex];
            Debug.Assert(method.DeclaringType != null);
            var parameters = method.GetParameters();
            var parameterTypes = new Type[parameters.Length];
            for (var i = 0; i < parameters.Length; i++)
                parameterTypes[i] = parameters[i].ParameterType;

            var attributes = MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig;
            if (method.DeclaringType.IsInterface)
                attributes |= MethodAttributes.Final | MethodAttributes.NewSlot;
            if ((method.Attributes & MethodAttributes.SpecialName) == MethodAttributes.SpecialName)
                attributes |= MethodAttributes.SpecialName;
            var methodBuilder =
                buildContext.ProxyType.DefineMethod(method.Name, attributes, method.ReturnType, parameterTypes);
            var gen = methodBuilder.GetILGenerator();

            // Prepare arguments for action invocation.

            // Load action field.
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldfld, buildContext.ActionField);

            // Load methods array field.
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldfld, buildContext.MethodsField);

            // Load index of method.
            gen.Emit(OpCodes.Ldc_I4, methodIndex);

            // Load array element.
            gen.Emit(OpCodes.Ldelem_Ref);

            if (parameters.Length > 0)
            {
                // Create array for action's parameters.
                gen.Emit(OpCodes.Ldc_I4, parameters.Length);
                gen.Emit(OpCodes.Newarr, typeof(object));

                // Fill array.
                // Load call arguments.
                for (var i = 0; i < parameters.Length; i++)
                {
                    gen.Emit(OpCodes.Dup);

                    // Parameter's index in array.
                    gen.Emit(OpCodes.Ldc_I4, i);

                    // Parameter's value.
                    gen.Emit(OpCodes.Ldarg, i + 1);
                    if (parameterTypes[i].IsValueType)
                    {
                        gen.Emit(OpCodes.Box, parameterTypes[i]);
                    }

                    // Set array's element
                    gen.Emit(OpCodes.Stelem_Ref);
                }
            }
            else
            {
                // Load static empty parameters field.
                gen.Emit(OpCodes.Ldsfld, buildContext.EmptyParametersField);
            }

            // Call action method.
            gen.Emit(OpCodes.Callvirt, InvokeMethod);

            // Load result.
            if (method.ReturnType != typeof(void))
            {
                if (method.ReturnType.IsValueType)
                    gen.Emit(OpCodes.Unbox_Any, method.ReturnType);
            }
            else
            {
                // Method should not return result, so remove result from stack.
                gen.Emit(OpCodes.Pop);
            }
            //exit
            gen.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Proxy build state.
        /// </summary>
        private class ProxyBuildContext
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="ProxyBuildContext"/> class.
            /// </summary>
            public ProxyBuildContext(TypeBuilder proxyType, Type serviceType)
            {
                ProxyType = proxyType;
                ServiceType = serviceType;
            }

            /** */ public TypeBuilder ProxyType { get; private set; }
            /** */ public Type ServiceType { get; private set; }

            /** */ public FieldBuilder MethodsField { get; set; }
            /** */ public FieldBuilder EmptyParametersField { get; set; }
            /** */ public FieldBuilder ActionField { get; set; }

            /** */ public MethodInfo[] Methods { get; set; }
        }
    }
}
