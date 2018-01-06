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
    using System.Reflection;
    using System.Reflection.Emit;
    using ProxyAction = System.Func<System.Reflection.MethodBase, object[], object>;

    internal static class ServiceProxyTypeGenerator
    {
        private static readonly Type ActionType = typeof(ProxyAction);

        private static readonly MethodInfo InvokeMethod = ActionType.GetMethod("Invoke");

        private static readonly ModuleBuilder ModuleBuilder = CreateModuleBuilder();

        public static ProxyTypeGenerationResult Generate(Type serviceType)
        {
            if (serviceType == null) throw new ArgumentNullException("serviceType");
            var isClass = serviceType.IsClass;
            var proxyType = ModuleBuilder.DefineType(
                string.Format("{0}Proxy", serviceType.FullName),
                TypeAttributes.Class, isClass ? serviceType : null);
            var buildContext = new ProxyBuildContext(proxyType, serviceType);
            if (!isClass)
                proxyType.AddInterfaceImplementation(serviceType);

            GenerateFields(buildContext);
            GenerateStaticConstructor(buildContext);
            GenerateConstructor(buildContext);

            buildContext.Methods = ServiceMethodHelper.GetVirtualMethods(buildContext.ServiceType);
            for (var i = 0; i < buildContext.Methods.Length; i++)
                GenerateMethod(buildContext, i);

            var type = proxyType.CreateType();
            return new ProxyTypeGenerationResult(type, buildContext.Methods);
        }

        private static ModuleBuilder CreateModuleBuilder()
        {
            var name = Guid.NewGuid().ToString("N");
#if !NETCOREAPP2_0
            var assemblyBuilder =
                AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name),
                    AssemblyBuilderAccess.RunAndCollect);
#else
            var assemblyBuilder =
                AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(name),
                    AssemblyBuilderAccess.RunAndCollect);
#endif
            return assemblyBuilder.DefineDynamicModule(name);
        }

        private static void GenerateFields(ProxyBuildContext buildContext)
        {
            //static field - empty object array to optimize calls without parameters
            buildContext.EmptyParametersField = buildContext.ProxyType.DefineField("_emptyParameters", typeof(object[]),
                FieldAttributes.Static | FieldAttributes.Private | FieldAttributes.InitOnly);
            //instance field for function to invoke
            buildContext.ActionField = buildContext.ProxyType.DefineField("_action", ActionType,
                FieldAttributes.Private | FieldAttributes.InitOnly);
            //field - array with methods of service's type
            buildContext.MethodsField = buildContext.ProxyType.DefineField("_methods", typeof(MethodInfo[]),
                FieldAttributes.Private | FieldAttributes.InitOnly);
        }

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
                //load "this"
                gen.Emit(OpCodes.Ldarg_0);
                //call base constructor
                gen.Emit(OpCodes.Call, baseCtr);
            }

            //assign parameters to fields
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldarg_1);
            gen.Emit(OpCodes.Stfld, buildContext.ActionField);

            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldarg_2);
            gen.Emit(OpCodes.Stfld, buildContext.MethodsField);

            gen.Emit(OpCodes.Ret);
        }

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

            //prepare arguments for action invocation

            //load action field
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldfld, buildContext.ActionField);

            //load methods array field
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldfld, buildContext.MethodsField);
            //load index of method
            gen.Emit(OpCodes.Ldc_I4, methodIndex);
            //load element
            gen.Emit(OpCodes.Ldelem_Ref);

            if (parameters.Length > 0)
            {
                //create array for action's parameters
                gen.Emit(OpCodes.Ldc_I4, parameters.Length);
                gen.Emit(OpCodes.Newarr, typeof(object));

                //fill array
                //load call arguments
                for (var i = 0; i < parameters.Length; i++)
                {
                    gen.Emit(OpCodes.Dup);
                    //parameter's index in array
                    gen.Emit(OpCodes.Ldc_I4, i);
                    //parameter's value
                    gen.Emit(OpCodes.Ldarg, i + 1);
                    if (parameterTypes[i].IsValueType)
                        gen.Emit(OpCodes.Box, parameterTypes[i]);
                    //set array's element
                    gen.Emit(OpCodes.Stelem_Ref);
                }
            }
            else
            {
                //load static empty parameters field
                gen.Emit(OpCodes.Ldsfld, buildContext.EmptyParametersField);
            }

            //call action method
            gen.Emit(OpCodes.Callvirt, InvokeMethod);

            //load result
            if (method.ReturnType != typeof(void))
            {
                if (method.ReturnType.IsValueType)
                    gen.Emit(OpCodes.Unbox_Any, method.ReturnType);
            }
            else
            {
                //method should not return result. so, remove result from stack
                gen.Emit(OpCodes.Pop);
            }
            //exit
            gen.Emit(OpCodes.Ret);
        }

        private class ProxyBuildContext
        {
            public ProxyBuildContext(TypeBuilder proxyType, Type serviceType)
            {
                ProxyType = proxyType;
                ServiceType = serviceType;
            }

            public TypeBuilder ProxyType { get; private set; }
            public Type ServiceType { get; private set; }

            public FieldBuilder MethodsField { get; set; }
            public FieldBuilder EmptyParametersField { get; set; }
            public FieldBuilder ActionField { get; set; }

            public MethodInfo[] Methods { get; set; }
        }
    }
}