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
    using System.Text;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Base class for API parity tests.
    /// </summary>
    public static class ParityTest
    {
        /** Property regex. */
        private static readonly Regex JavaPropertyRegex = 
            new Regex("(@Deprecated)?\\s+public [^=^\r^\n]+ (\\w+)\\(\\) {", RegexOptions.Compiled);

        /** Properties that are not needed on .NET side. */
        private static readonly string[] UnneededProperties =
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
            var path = Path.Combine(IgniteHome.Resolve(null), javaFilePath);

            Assert.IsTrue(File.Exists(path));

            var dotNetProperties = type.GetProperties()
                .ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

            var javaProperties = GetJavaProperties(path)
                .Except(excludedProperties ?? Enumerable.Empty<string>());

            var missingProperties = javaProperties
                .Where(jp => !GetNameVariants(jp, knownMappings).Any(dotNetProperties.ContainsKey))
                .ToDictionary(x => x, x => x, StringComparer.OrdinalIgnoreCase);

            var knownMissing = (knownMissingProperties ?? Enumerable.Empty<string>())
                .ToDictionary(x => x, x => x, StringComparer.OrdinalIgnoreCase);

            var sb = new StringBuilder();

            foreach (var javaMissingProp in missingProperties)
            {
                if (!knownMissing.ContainsKey(javaMissingProp.Key))
                {
                    sb.AppendFormat("{0}.{1} property is missing in .NET.\n", type.Name, javaMissingProp.Key);
                }
            }

            foreach (var dotnetMissingProp in knownMissing)
            {
                if (!missingProperties.ContainsKey(dotnetMissingProp.Key))
                {
                    sb.AppendFormat("{0}.{1} property is missing in Java, but is specified as known in .NET.\n", 
                        type.Name, dotnetMissingProp.Key);
                }
            }

            if (sb.Length > 0)
            {
                Assert.Fail(sb.ToString());
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
                .Except(UnneededProperties);
        }

        /// <summary>
        /// Gets the name variants for a property.
        /// </summary>
        private static IEnumerable<string> GetNameVariants(string javaPropertyName, 
            IDictionary<string, string> knownMappings)
        {
            yield return javaPropertyName;

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
