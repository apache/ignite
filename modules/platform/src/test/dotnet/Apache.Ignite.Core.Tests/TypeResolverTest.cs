/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Tests.TestDll;
    using GridGain.Impl.Portable;

    using NUnit.Framework;

    /// <summary>
    /// <see cref="TypeResolver"/> tests.
    /// </summary>
    public class TypeResolverTest
    {
        /// <summary>
        /// Tests generic type resolve.
        /// </summary>
        [Test]
        public void TestGenerics()
        {
            var testTypes = new[]
            {
                typeof (TestGenericPortable<int>),
                typeof (TestGenericPortable<string>),
                typeof (TestGenericPortable<TestGenericPortable<int>>),
                typeof (TestGenericPortable<List<Tuple<int, string>>>),
                typeof (TestGenericPortable<List<TestGenericPortable<List<Tuple<int, string>>>>>),
                typeof (List<TestGenericPortable<List<TestGenericPortable<List<Tuple<int, string>>>>>>),
                typeof (TestGenericPortable<int, string>),
                typeof (TestGenericPortable<int, TestGenericPortable<string>>),
                typeof (TestGenericPortable<int, string, Type>),
                typeof (TestGenericPortable<int, string, TestGenericPortable<int, string, Type>>)
            };

            foreach (var type in testTypes)
            {
                // Without assembly
                var resolvedType = new TypeResolver().ResolveType(type.FullName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);
                
                // With assembly
                resolvedType = new TypeResolver().ResolveType(type.FullName, type.Assembly.FullName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);

                // Assembly-qualified
                resolvedType = new TypeResolver().ResolveType(type.AssemblyQualifiedName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);
            }
        }

        /// <summary>
        /// Tests loading a type from referenced assembly that is not yet loaded.
        /// </summary>
        [Test]
        public void TestReferencedAssemblyLoading()
        {
            const string dllName = "GridGainTestDll";

            const string typeName = "GridGain.TestDll.TestClass";

            // Check that the dll is not yet loaded
            Assert.IsFalse(AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.StartsWith(dllName)));

            // Check that the dll is referenced by current assembly
            Assert.IsTrue(Assembly.GetExecutingAssembly().GetReferencedAssemblies()
                .Any(x => x.FullName.StartsWith(dllName)));

            // Check resolver
            var type = new TypeResolver().ResolveType(typeName);
            
            Assert.IsNotNull(type);
            Assert.AreEqual(typeName, type.FullName);
            Assert.IsNotNull(Activator.CreateInstance(type));

            // At this moment the dll should be loaded
            Assert.IsTrue(AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.StartsWith(dllName)));
        }

        /// <summary>
        /// Unused method that forces C# compiler to include TestDll assembly reference.
        /// Without this, compiler will remove the reference as unused.
        /// However, since it is never called, TestDll does not get loaded.
        /// </summary>
        public void UnusedMethod()
        {
            Assert.IsNotNull(typeof(TestClass));
        }        
    }
}