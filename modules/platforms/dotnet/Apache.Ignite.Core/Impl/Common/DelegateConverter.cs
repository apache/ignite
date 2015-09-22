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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Diagnostics;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Reflection.Emit;

    /// <summary>
    /// Converts generic and non-generic delegates.
    /// </summary>
    public static class DelegateConverter
    {
        /** */
        private const string DefaultMethodName = "Invoke";
        
        /// <summary>
        /// Compiles a function without arguments.
        /// </summary>
        /// <param name="targetType">Type of the target.</param>
        /// <returns>Compiled function that calls specified method on specified target.</returns>
        public static Func<object, object> CompileFunc(Type targetType)
        {
            var method = targetType.GetMethod(DefaultMethodName);

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, targetType);

            var callExpr = Expression.Call(targetParamConverted, method);
            var convertResultExpr = Expression.Convert(callExpr, typeof(object));

            return Expression.Lambda<Func<object, object>>(convertResultExpr, targetParam).Compile();
        }

        /// <summary>
        /// Compiles a function with arbitrary number of arguments.
        /// </summary>
        /// <typeparam name="T">Resulting delegate type.</typeparam>
        /// <param name="targetType">Type of the target.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <param name="convertToObject">
        /// Flags that indicate whether func params and/or return value should be converted from/to object.
        /// </param>
        /// <param name="methodName">Name of the method.</param>
        /// <returns>
        /// Compiled function that calls specified method on specified target.
        /// </returns>
        public static T CompileFunc<T>(Type targetType, Type[] argTypes, bool[] convertToObject = null,
            string methodName = null)
            where T : class
        {
            var method = targetType.GetMethod(methodName ?? DefaultMethodName, argTypes);

            return CompileFunc<T>(targetType, method, argTypes, convertToObject);
        }

        /// <summary>
        /// Compiles a function with arbitrary number of arguments.
        /// </summary>
        /// <typeparam name="T">Resulting delegate type.</typeparam>
        /// <param name="method">Method.</param>
        /// <param name="targetType">Type of the target.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <param name="convertToObject">
        /// Flags that indicate whether func params and/or return value should be converted from/to object.
        /// </param>
        /// <returns>
        /// Compiled function that calls specified method on specified target.
        /// </returns>
        public static T CompileFunc<T>(Type targetType, MethodInfo method, Type[] argTypes, 
            bool[] convertToObject = null)
            where T : class
        {
            if (argTypes == null)
            {
                var args = method.GetParameters();
                argTypes = new Type[args.Length];

                for (int i = 0; i < args.Length; i++)
                    argTypes[i] = args[i].ParameterType;
            }

            Debug.Assert(convertToObject == null || (convertToObject.Length == argTypes.Length + 1));
            Debug.Assert(method != null);

            targetType = method.IsStatic ? null : (targetType ?? method.DeclaringType);

            var targetParam = Expression.Parameter(typeof(object));
            
            Expression targetParamConverted = null;
            ParameterExpression[] argParams;
            int argParamsOffset = 0;

            if (targetType != null)
            {
                targetParamConverted = Expression.Convert(targetParam, targetType);
                argParams = new ParameterExpression[argTypes.Length + 1];
                argParams[0] = targetParam;
                argParamsOffset = 1;
            }
            else
                argParams = new ParameterExpression[argTypes.Length];  // static method

            var argParamsConverted = new Expression[argTypes.Length];

            for (var i = 0; i < argTypes.Length; i++)
            {
                if (convertToObject == null || convertToObject[i])
                {
                    var argParam = Expression.Parameter(typeof (object));
                    argParams[i + argParamsOffset] = argParam;
                    argParamsConverted[i] = Expression.Convert(argParam, argTypes[i]);
                }
                else
                {
                    var argParam = Expression.Parameter(argTypes[i]);
                    argParams[i + argParamsOffset] = argParam;
                    argParamsConverted[i] = argParam;
                }
            }

            Expression callExpr = Expression.Call(targetParamConverted, method, argParamsConverted);

            if (convertToObject == null || convertToObject[argTypes.Length])
                callExpr = Expression.Convert(callExpr, typeof(object));

            return Expression.Lambda<T>(callExpr, argParams).Compile();
        }

        /// <summary>
        /// Compiles a generic ctor with arbitrary number of arguments.
        /// </summary>
        /// <typeparam name="T">Result func type.</typeparam>
        /// <param name="type">Type to be created by ctor.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <param name="convertResultToObject">if set to <c>true</c> [convert result to object].
        /// Flag that indicates whether ctor return value should be converted to object.
        /// </param>
        /// <returns>
        /// Compiled generic constructor.
        /// </returns>
        public static T CompileCtor<T>(Type type, Type[] argTypes, bool convertResultToObject = true)
        {
            var ctor = type.GetConstructor(argTypes);

            Debug.Assert(ctor != null);

            var args = new ParameterExpression[argTypes.Length];
            var argsConverted = new Expression[argTypes.Length];

            for (var i = 0; i < argTypes.Length; i++)
            {
                var arg = Expression.Parameter(typeof(object));
                args[i] = arg;
                argsConverted[i] = Expression.Convert(arg, argTypes[i]);
            }

            Expression ctorExpr = Expression.New(ctor, argsConverted);  // ctor takes args of specific types

            if (convertResultToObject)
                ctorExpr = Expression.Convert(ctorExpr, typeof (object)); // convert ctor result to object

            return Expression.Lambda<T>(ctorExpr, args).Compile();  // lambda takes args as objects
        }

        /// <summary>
        /// Compiles the field setter.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>Compiled field setter.</returns>
        public static Action<object, object> CompileFieldSetter(FieldInfo field)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);

            var valParam = Expression.Parameter(typeof(object));
            var valParamConverted = Expression.Convert(valParam, field.FieldType);

            var assignExpr = Expression.Call(GetWriteFieldMethod(field), targetParamConverted, valParamConverted);

            return Expression.Lambda<Action<object, object>>(assignExpr, targetParam, valParam).Compile();
        }

        /// <summary>
        /// Compiles the property setter.
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>Compiled property setter.</returns>
        public static Action<object, object> CompilePropertySetter(PropertyInfo prop)
        {
            Debug.Assert(prop != null);
            Debug.Assert(prop.DeclaringType != null);   // non-static

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, prop.DeclaringType);

            var valParam = Expression.Parameter(typeof(object));
            var valParamConverted = Expression.Convert(valParam, prop.PropertyType);

            var fld = Expression.Property(targetParamConverted, prop);

            var assignExpr = Expression.Assign(fld, valParamConverted);

            return Expression.Lambda<Action<object, object>>(assignExpr, targetParam, valParam).Compile();
        }

        /// <summary>
        /// Gets a method to write a field (including private and readonly).
        /// NOTE: Expression Trees can't write readonly fields.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>Resulting MethodInfo.</returns>
        public static DynamicMethod GetWriteFieldMethod(FieldInfo field)
        {
            Debug.Assert(field != null);

            var module = Assembly.GetExecutingAssembly().GetModules()[0];

            var method = new DynamicMethod(string.Empty, null, new[] { field.DeclaringType, field.FieldType }, module,
                true);

            var il = method.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Stfld, field);
            il.Emit(OpCodes.Ret);

            return method;
        }

    }
}