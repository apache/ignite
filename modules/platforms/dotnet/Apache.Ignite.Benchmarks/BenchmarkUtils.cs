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

namespace Apache.Ignite.Benchmarks
{
    using System;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using Apache.Ignite.Benchmarks.Model;

    /// <summary>
    /// Utility methods for benchmarks.
    /// </summary>
    internal static class BenchmarkUtils
    {
        /** Property binding flags. */
        public static BindingFlags PROP_FLAGS = BindingFlags.Instance | BindingFlags.Public;

        /** Thread-local random. */
        private static readonly ThreadLocal<Random> RAND;

        /** Cached ANSI chcracters. */
        private static readonly char[] CHARS;

        /** Seed to randoms. */
        private static int SEED_CTR;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static BenchmarkUtils()
        {
            RAND = new ThreadLocal<Random>(() => {
                int seed = Interlocked.Add(ref SEED_CTR, 100);

                return new Random(seed); 
            });

            CHARS = new char[10 + 26 + 26];

            int pos = 0;

            for (char i = '0'; i < '0' + 10; i++)
                CHARS[pos++] = i;

            for (char i = 'A'; i < 'A' + 26; i++)
                CHARS[pos++] = i;

            for (char i = 'a'; i < 'a' + 26; i++)
                CHARS[pos++] = i;
        }

        /// <summary>
        /// Generate random integer.
        /// </summary>
        /// <param name="max">Maximum value (exclusive).</param>
        /// <returns></returns>
        public static int RandomInt(int max)
        {
            return RandomInt(0, max);
        }

        /// <summary>
        /// Generate random integer.
        /// </summary>
        /// <param name="min">Minimum value (inclusive).</param>
        /// <param name="max">Maximum value (exclusive).</param>
        /// <returns></returns>
        public static int RandomInt(int min, int max)
        {
            return RAND.Value.Next(min, max);
        }

        /// <summary>
        /// Generate random string.
        /// </summary>
        /// <param name="len">Length.</param>
        /// <returns>String.</returns>
        public static string RandomString(int len)
        {
            Random rand = RAND.Value;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < len; i++)
                sb.Append(CHARS[rand.Next(CHARS.Length)]);

            return sb.ToString();
        }

        /// <summary>
        /// Generate random address.
        /// </summary>
        /// <returns>Address.</returns>
        public static Address RandomAddress()
        {
            return new Address(
                RandomString(15),
                RandomString(20),
                RandomInt(1, 500),
                RandomInt(1, 35)
            );
        }

        /// <summary>
        /// Generate random company.
        /// </summary>
        /// <returns>Company.</returns>
        public static Company RandomCompany()
        {
            return new Company(
                RandomInt(0, 100),
                RandomString(20),
                RandomInt(100, 3000),
                RandomAddress(),
                RandomString(20)
            );
        }

        /// <summary>
        /// Generate random employee.
        /// </summary>
        /// <param name="payload">Payload size.</param>
        /// <returns>Employee.</returns>
        public static Employee RandomEmployee(int payload)
        {
            return new Employee(
                RandomInt(0, 1000),
                RandomString(15),
                RandomInt(0, 1000),
                RandomInt(18, 60),
                (Sex)RandomInt(0, 1),
                (long)RandomInt(10000, 30000),
                RandomAddress(),
                (Department)RandomInt(0, 5),
                payload
            );
        }

        /// <summary>
        /// List all properties present in the given object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>Properties.</returns>
        public static PropertyInfo[] ListProperties(object obj)
        {
            return obj.GetType().GetProperties(PROP_FLAGS);
        }

        /// <summary>
        /// Find property with the given name in the object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="name">Name.</param>
        /// <returns>Property.</returns>
        public static PropertyInfo FindProperty(object obj, string name) {
            PropertyInfo[] props = ListProperties(obj);

            name = name.ToLower();

            foreach (PropertyInfo prop in props)
            {
                if (prop.Name.ToLower().Equals(name))
                    return prop;
            }

            return null;
        }

        /// <summary>
        /// Set property on the given object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="prop">Property.</param>
        /// <param name="val">Value.</param>
        public static void SetProperty(object obj, PropertyInfo prop, string val)
        {
            object val0;

            Type propType = prop.PropertyType;

            if (propType == typeof(int))
            {
                try
                {
                    val0 = int.Parse(val);
                }
                catch (Exception e)
                {
                    throw new Exception("Failed to parse property value [property=" + prop.Name +
                        ", value=" + val + ']', e);
                }
            }
            else if (propType == typeof(long))
            {
                try
                {
                    val0 = long.Parse(val);
                }
                catch (Exception e)
                {
                    throw new Exception("Failed to parse property value [property=" + prop.Name +
                        ", value=" + val + ']', e);
                }
            }
            else if (propType == typeof(bool))
            {
                try
                {
                    val0 = bool.Parse(val);
                }
                catch (Exception e)
                {
                    throw new Exception("Failed to parse property value [property=" + prop.Name +
                        ", value=" + val + ']', e);
                }
            }
            else if (propType == typeof(string))
                val0 = val;                            
            else
                throw new Exception("Unsupported property type [property=" + prop.Name +
                    ", type=" + propType.Name + ']');

            prop.SetValue(obj, val0, null);
        }
    }
}
