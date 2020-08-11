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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Reflection.Emit;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Converts generic and non-generic delegates.
    /// </summary>
    public static class DelegateConverter
    {
        /** */
        private const string DefaultMethodName = "Invoke";

        /** */
        private static readonly MethodInfo ReadObjectMethod = typeof (IBinaryRawReader).GetMethod("ReadObject");

        /** */
        private static readonly MethodInfo ConvertArrayMethod = typeof(DelegateConverter).GetMethod("ConvertArray",
            BindingFlags.Static | BindingFlags.NonPublic);

        /// <summary>
        /// Compiles a function without arguments.
        /// </summary>
        /// <param name="targetType">Type of the target.</param>
        /// <returns>Compiled function that calls specified method on specified target.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Func<object, object> CompileFunc(Type targetType)
        {
            var method = targetType.GetMethod(DefaultMethodName);
            Debug.Assert(method != null, "method != null");

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
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
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
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
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
        /// Compiles a function with a single object[] argument which maps array items to actual arguments.
        /// </summary>
        /// <param name="method">Method.</param>
        /// <returns>
        /// Compiled function that calls specified method.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Func<object, object[], object> CompileFuncFromArray(MethodInfo method)
        {
            Debug.Assert(method != null);
            Debug.Assert(method.DeclaringType != null);

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, method.DeclaringType);

            var arrParam = Expression.Parameter(typeof(object[]));

            var methodParams = method.GetParameters();
            var argParams = new Expression[methodParams.Length];

            for (var i = 0; i < methodParams.Length; i++)
            {
                var arrElem = Expression.ArrayIndex(arrParam, Expression.Constant(i));
                argParams[i] = Convert(arrElem, methodParams[i].ParameterType);
            }

            Expression callExpr = Expression.Call(targetParamConverted, method, argParams);

            if (callExpr.Type == typeof(void))
            {
                // Convert action to function
                var action = Expression.Lambda<Action<object, object[]>>(callExpr, targetParam, arrParam).Compile();
                return (obj, args) =>
                {
                    action(obj, args);
                    return null;
                };
            }

            callExpr = Expression.Convert(callExpr, typeof(object));

            return Expression.Lambda<Func<object, object[], object>>(callExpr, targetParam, arrParam).Compile();
        }

        /// <summary>
        /// Compiles a generic ctor with arbitrary number of arguments.
        /// </summary>
        /// <typeparam name="T">Result func type.</typeparam>
        /// <param name="ctor">Constructor info.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <param name="convertResultToObject">
        /// Flag that indicates whether ctor return value should be converted to object.</param>
        /// <param name="convertParamsFromObject">
        /// Flag that indicates whether ctor args are object and should be converted to concrete type.</param>
        /// <returns>
        /// Compiled generic constructor.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static T CompileCtor<T>(ConstructorInfo ctor, Type[] argTypes, bool convertResultToObject = true,
            bool convertParamsFromObject = true)
        {
            Debug.Assert(ctor != null);

            var args = new ParameterExpression[argTypes.Length];
            var argsConverted = new Expression[argTypes.Length];

            for (var i = 0; i < argTypes.Length; i++)
            {
                if (convertParamsFromObject)
                {
                    var arg = Expression.Parameter(typeof(object));
                    args[i] = arg;
                    argsConverted[i] = Expression.Convert(arg, argTypes[i]);
                }
                else
                {
                    argsConverted[i] = args[i] = Expression.Parameter(argTypes[i]);
                }
            }

            Expression ctorExpr = Expression.New(ctor, argsConverted);  // ctor takes args of specific types

            if (convertResultToObject)
                ctorExpr = Expression.Convert(ctorExpr, typeof(object)); // convert ctor result to object

            return Expression.Lambda<T>(ctorExpr, args).Compile();  // lambda takes args as objects
        }

        /// <summary>
        /// Compiles a generic ctor with arbitrary number of arguments
        /// that takes an uninitialized object as a first arguments.
        /// </summary>
        /// <typeparam name="T">Result func type.</typeparam>
        /// <param name="ctor">Constructor info.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <returns>
        /// Compiled generic constructor.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static T CompileUninitializedObjectCtor<T>(ConstructorInfo ctor, Type[] argTypes)
        {
            Debug.Assert(ctor != null);
            Debug.Assert(ctor.DeclaringType != null);
            Debug.Assert(argTypes != null);

            argTypes = new[] {typeof(object)}.Concat(argTypes).ToArray();

            var helperMethod = new DynamicMethod(string.Empty, typeof(void), argTypes, ctor.Module, true);
            var il = helperMethod.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);

            if (ctor.DeclaringType.IsValueType)
                il.Emit(OpCodes.Unbox, ctor.DeclaringType);   // modify boxed copy

            if (argTypes.Length > 1)
                il.Emit(OpCodes.Ldarg_1);

            if (argTypes.Length > 2)
                il.Emit(OpCodes.Ldarg_2);

            if (argTypes.Length > 3)
                throw new NotSupportedException("Not supported: too many ctor args.");

            il.Emit(OpCodes.Call, ctor);
            il.Emit(OpCodes.Ret);

            var constructorInvoker = helperMethod.CreateDelegate(typeof(T));

            return (T) (object) constructorInvoker;
        }

        /// <summary>
        /// Compiles a generic ctor with arbitrary number of arguments.
        /// </summary>
        /// <typeparam name="T">Result func type.</typeparam>
        /// <param name="type">Type to be created by ctor.</param>
        /// <param name="argTypes">Argument types.</param>
        /// <param name="convertResultToObject">
        /// Flag that indicates whether ctor return value should be converted to object.
        /// </param>
        /// <returns>
        /// Compiled generic constructor.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static T CompileCtor<T>(Type type, Type[] argTypes, bool convertResultToObject = true)
        {
            var ctor = type.GetConstructor(argTypes);

            return CompileCtor<T>(ctor, argTypes, convertResultToObject);
        }

        /// <summary>
        /// Compiles a constructor that reads all arguments from a binary reader.
        /// </summary>
        /// <typeparam name="T">Result type</typeparam>
        /// <param name="ctor">The ctor.</param>
        /// <param name="innerCtorFunc">Function to retrieve reading constructor for an argument.
        /// Can be null or return null, in this case the argument will be read directly via ReadObject.</param>
        /// <returns></returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Func<IBinaryRawReader, T> CompileCtor<T>(ConstructorInfo ctor,
            Func<Type, ConstructorInfo> innerCtorFunc)
        {
            Debug.Assert(ctor != null);

            var readerParam = Expression.Parameter(typeof (IBinaryRawReader));

            var ctorExpr = GetConstructorExpression(ctor, innerCtorFunc, readerParam, typeof(T));

            return Expression.Lambda<Func<IBinaryRawReader, T>>(ctorExpr, readerParam).Compile();
        }

        /// <summary>
        /// Gets the constructor expression.
        /// </summary>
        /// <param name="ctor">The ctor.</param>
        /// <param name="innerCtorFunc">The inner ctor function.</param>
        /// <param name="readerParam">The reader parameter.</param>
        /// <param name="resultType">Type of the result.</param>
        /// <returns>
        /// Ctor call expression.
        /// </returns>
        private static Expression GetConstructorExpression(ConstructorInfo ctor,
            Func<Type, ConstructorInfo> innerCtorFunc, Expression readerParam, Type resultType)
        {
            var ctorParams = ctor.GetParameters();

            var paramsExpr = new List<Expression>(ctorParams.Length);

            foreach (var param in ctorParams)
            {
                var paramType = param.ParameterType;

                var innerCtor = innerCtorFunc != null ? innerCtorFunc(paramType) : null;

                if (innerCtor != null)
                {
                    var readExpr = GetConstructorExpression(innerCtor, innerCtorFunc, readerParam, paramType);

                    paramsExpr.Add(readExpr);
                }
                else
                {
                    var readMethod = ReadObjectMethod.MakeGenericMethod(paramType);

                    var readExpr = Expression.Call(readerParam, readMethod);

                    paramsExpr.Add(readExpr);
                }
            }

            Expression ctorExpr = Expression.New(ctor, paramsExpr);

            ctorExpr = Expression.Convert(ctorExpr, resultType);

            return ctorExpr;
        }

        /// <summary>
        /// Compiles the field setter.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>Compiled field setter.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Action<object, object> CompileFieldSetter(FieldInfo field)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);

            var targetParam = Expression.Parameter(typeof(object));
            var valParam = Expression.Parameter(typeof(object));
            var valParamConverted = Expression.Convert(valParam, field.FieldType);

            var assignExpr = Expression.Call(GetWriteFieldMethod(field), targetParam, valParamConverted);

            return Expression.Lambda<Action<object, object>>(assignExpr, targetParam, valParam).Compile();
        }

        /// <summary>
        /// Compiles the property setter.
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>Compiled property setter.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Action<object, object> CompilePropertySetter(PropertyInfo prop)
        {
            Debug.Assert(prop != null);
            Debug.Assert(prop.DeclaringType != null);

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, prop.DeclaringType);

            var valParam = Expression.Parameter(typeof(object));
            var valParamConverted = Expression.Convert(valParam, prop.PropertyType);

            var fld = Expression.Property(targetParamConverted, prop);

            var assignExpr = Expression.Assign(fld, valParamConverted);

            return Expression.Lambda<Action<object, object>>(assignExpr, targetParam, valParam).Compile();
        }

        /// <summary>
        /// Compiles the property setter.
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>Compiled property setter.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Func<object, object> CompilePropertyGetter(PropertyInfo prop)
        {
            Debug.Assert(prop != null);
            Debug.Assert(prop.DeclaringType != null);

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = prop.GetGetMethod().IsStatic
                ? null
                // ReSharper disable once AssignNullToNotNullAttribute (incorrect warning)
                : Expression.Convert(targetParam, prop.DeclaringType);

            var fld = Expression.Property(targetParamConverted, prop);

            var fldConverted = Expression.Convert(fld, typeof (object));

            return Expression.Lambda<Func<object, object>>(fldConverted, targetParam).Compile();
        }

        /// <summary>
        /// Compiles the property setter.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>Compiled property setter.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static Func<object, object> CompileFieldGetter(FieldInfo field)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);

            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = field.IsStatic
                ? null
                : Expression.Convert(targetParam, field.DeclaringType);

            var fld = Expression.Field(targetParamConverted, field);

            var fldConverted = Expression.Convert(fld, typeof (object));

            return Expression.Lambda<Func<object, object>>(fldConverted, targetParam).Compile();
        }

        /// <summary>
        /// Gets a method to write a field (including private and readonly).
        /// NOTE: Expression Trees can't write readonly fields.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>Resulting MethodInfo.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static DynamicMethod GetWriteFieldMethod(FieldInfo field)
        {
            Debug.Assert(field != null);

            var declaringType = field.DeclaringType;

            Debug.Assert(declaringType != null);

            var method = new DynamicMethod(string.Empty, null, new[] { typeof(object), field.FieldType },
                declaringType, true);

            var il = method.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);

            if (declaringType.IsValueType)
                il.Emit(OpCodes.Unbox, declaringType);   // modify boxed copy

            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Stfld, field);
            il.Emit(OpCodes.Ret);

            return method;
        }

        /// <summary>
        /// Gets the constructor with exactly matching signature.
        /// <para />
        /// Type.GetConstructor matches compatible ones (i.e. taking object instead of concrete type).
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="types">The argument types.</param>
        /// <returns>Constructor info.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public static ConstructorInfo GetConstructorExact(Type type, Type[] types)
        {
            Debug.Assert(type != null);
            Debug.Assert(types != null);

            foreach (var constructorInfo in type.GetConstructors(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                var ctorTypes = constructorInfo.GetParameters().Select(x => x.ParameterType);

                if (ctorTypes.SequenceEqual(types))
                    return constructorInfo;
            }

            return null;
        }

        /// <summary>
        /// Converts expression to a given type.
        /// </summary>
        private static Expression Convert(Expression value, Type targetType)
        {
            if (targetType.IsArray && targetType.GetElementType() != typeof(object))
            {
                var convertMethod = ConvertArrayMethod.MakeGenericMethod(targetType.GetElementType());

                var objArray = Expression.Convert(value, typeof(object[]));

                return Expression.Call(null, convertMethod, objArray);
            }

            return Expression.Convert(value, targetType);
        }

        /// <summary>
        /// Converts object array to typed array.
        /// </summary>
        // ReSharper disable once UnusedMember.Local (used by reflection).
        private static T[] ConvertArray<T>(object[] arr)
        {
            if (arr == null)
            {
                return null;
            }

            var res = new T[arr.Length];

            Array.Copy(arr, res, arr.Length);

            return res;
        }
    }
}
