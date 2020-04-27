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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Base class for API parity tests.
    /// </summary>
    public static class ParityTest
    {
        /** Test ignore reason: we should not fail builds due to new APIs being added in Java. */
        public const string IgnoreReason = "API parity tests are supposed to be run manually.";

        /** Property regex. */
        private static readonly Regex JavaPropertyRegex = 
            new Regex("(@Deprecated)?\\s+public [^=^\r^\n]+ (\\w+)\\(\\) {", RegexOptions.Compiled);

        /** Interface method regex. */
        private static readonly Regex JavaInterfaceMethodRegex = 
            new Regex("(@Deprecated)?\\s+(@Override)?\\s+public [^=^\r^\n]+ (\\w+)\\(.*?\\)",
                RegexOptions.Compiled | RegexOptions.Singleline);

        /** Properties that are not needed on .NET side. */
        private static readonly string[] UnneededMethods =
        {
            "toString",
            "hashCode",
            "writeReplace"
        };

        /// <summary>
        /// Tests the configuration parity.
        /// </summary>
        public static void CheckConfigurationParity(string javaFilePath, 
            Type type, 
            IEnumerable<string> excludedProperties = null,
            IEnumerable<string> knownMissingProperties = null,
            Dictionary<string, string> knownMappings = null)
        {
            var path = GetFullPath(javaFilePath);

            var dotNetProperties = type.GetProperties()
                .ToDictionary(x => x.Name, x => (MemberInfo) x, StringComparer.OrdinalIgnoreCase);

            var javaProperties = GetJavaProperties(path)
                .Except(excludedProperties ?? Enumerable.Empty<string>());

            CheckParity(type, knownMissingProperties, knownMappings, javaProperties, dotNetProperties);
        }

        /// <summary>
        /// Tests the configuration parity.
        /// </summary>
        public static void CheckInterfaceParity(string javaFilePath, 
            Type type, 
            IEnumerable<string> excludedMembers = null,
            IEnumerable<string> knownMissingMembers = null,
            Dictionary<string, string> knownMappings = null)
        {
            var path = GetFullPath(javaFilePath);

            var dotNetMembers = GetMembers(type)
                .GroupBy(x => x.Name)
                .ToDictionary(x => x.Key, x => x.First(), StringComparer.OrdinalIgnoreCase);

            var javaMethods = GetJavaInterfaceMethods(path)
                .Except(excludedMembers ?? Enumerable.Empty<string>());

            CheckParity(type, knownMissingMembers, knownMappings, javaMethods, dotNetMembers);
        }

        /// <summary>
        /// Gets the members.
        /// </summary>
        private static IEnumerable<MemberInfo> GetMembers(Type type)
        {
            var types = new Stack<Type>();
            types.Push(type);

            while (types.Count > 0)
            {
                var t = types.Pop();

                foreach (var m in t.GetMembers())
                {
                    yield return m;
                }

                foreach (var i in t.GetInterfaces())
                {
                    types.Push(i);
                }
            }
        }

        /// <summary>
        /// Gets the full path.
        /// </summary>
        private static string GetFullPath(string javaFilePath)
        {
            javaFilePath = javaFilePath.Replace('\\', Path.DirectorySeparatorChar);

            var path = Path.Combine(IgniteHome.Resolve(), javaFilePath);
            Assert.IsTrue(File.Exists(path), path);

            return path;
        }

        /// <summary>
        /// Checks the parity.
        /// </summary>
        private static void CheckParity(Type type, IEnumerable<string> knownMissingMembers, 
            IDictionary<string, string> knownMappings, IEnumerable<string> javaMethods, 
            IDictionary<string, MemberInfo> dotNetMembers)
        {
            var missingMembers = javaMethods
                .Where(jp => !GetNameVariants(jp, knownMappings).Any(dotNetMembers.ContainsKey))
                .ToDictionary(x => x, x => x, StringComparer.OrdinalIgnoreCase);

            var knownMissing = (knownMissingMembers ?? Enumerable.Empty<string>())
                .ToDictionary(x => x, x => x, StringComparer.OrdinalIgnoreCase);

            var sb = new StringBuilder();
            var codeSb = new StringBuilder();

            foreach (var javaMissingProp in missingMembers)
            {
                if (!knownMissing.ContainsKey(javaMissingProp.Key))
                {
                    sb.AppendFormat("{0}.{1} member is missing in .NET.\n" +
                                    "For new functionality please file a .NET ticket and update MissingProperties " +
                                    "array accordingly with a link to that ticket.\n", type.Name, javaMissingProp.Key);

                    codeSb.AppendFormat("\"{0}\", ", javaMissingProp.Key);
                }
            }

            foreach (var dotnetMissingProp in knownMissing)
            {
                if (!missingMembers.ContainsKey(dotnetMissingProp.Key))
                {
                    sb.AppendFormat("{0}.{1} member is missing in Java, but is specified as known in .NET.\n",
                        type.Name, dotnetMissingProp.Key);
                }
            }

            if (sb.Length > 0)
            {
                Assert.Fail(sb + "\nQuoted list: " + codeSb);
            }
        }

        /// <summary>
        /// Gets the java properties from file.
        /// </summary>
        private static IEnumerable<string> GetJavaProperties(string path)
        {
            var text = File.ReadAllText(path);

            return JavaPropertyRegex.Matches(text)
                .OfType<Match>()
                .Where(m => m.Groups[1].Value == string.Empty)
                .Select(m => m.Groups[2].Value.Replace("get", ""))
                .Where(x => !x.Contains(" void "))
                .Except(UnneededMethods);
        }

        /// <summary>
        /// Gets the java interface methods from file.
        /// </summary>
        private static IEnumerable<string> GetJavaInterfaceMethods(string path)
        {
            var text = File.ReadAllText(path);

            return JavaInterfaceMethodRegex.Matches(text)
                .OfType<Match>()
                .Where(m => m.Groups[1].Value == string.Empty)
                .Select(m => m.Groups[3].Value.Replace("get", ""))
                .Except(UnneededMethods);
        }

        /// <summary>
        /// Gets the name variants for a property.
        /// </summary>
        private static IEnumerable<string> GetNameVariants(string javaPropertyName, 
            IDictionary<string, string> knownMappings)
        {
            yield return javaPropertyName;
            
            yield return "get" + javaPropertyName;
            
            yield return "is" + javaPropertyName;

            yield return javaPropertyName.Replace("PoolSize", "ThreadPoolSize");

            if (javaPropertyName.StartsWith("is"))
            {
                yield return javaPropertyName.Substring(2);
            }

            string map;

            if (knownMappings != null && knownMappings.TryGetValue(javaPropertyName, out map))
            {
                yield return map;
            }
        }
    }
}
