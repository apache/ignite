// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Collections.Generic;
    using NUnit.Framework;

    using Dbg = System.Diagnostics.Debug;
    
    /** <summary>Test for Java hash code calculator class.</summary> */
    [TestFixture]
    public class GridClientJavaHelperTest {
        /** <summary>Validate known Java hash codes.</summary> */
        [Test]
        public void TestJavaHashes() {
            IDictionary<Object, Int32> map = new Dictionary<Object, Int32>();

            // Primitives.
            for (long i = 1, max = 1L << 48; i < max; i *= -3) {
                map.Add((byte)i, (int)(byte)i);
                map.Add((sbyte)i, (int)(sbyte)i);
                map.Add((char)i, (int)(char)i);
                map.Add((short)i, (int)(short)i);
                map.Add((ushort)i, (int)(ushort)i);
                map.Add((int)i, (int)i);
                map.Add((uint)i, (int)(uint)i);
                map.Add((long)i, (int)(i ^ (i >> 32)));
                map.Add((ulong)i, (int)(((ulong)i) ^ (((ulong)i) >> 32)));
            }

            // Double.
            map.Add(0.0, 0);
            map.Add(1.0, 1072693248);
            map.Add(-1.0, -1074790400);
            map.Add(3.1415e200, 1130072580);
            map.Add(3.1415e-200, -819810675);

            // Strings
            map.Add("", 0);
            map.Add("asdf", 3003444);
            map.Add("Hadoop\u3092\u6bba\u3059", 2113729932);
            map.Add("224ea4cd-f449-4dcb-869a-5317c63bd619", 258755163);
            map.Add("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", -863611257);
            map.Add("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", -1499939567);
            map.Add("d8f1f916-4357-4cfe-a7df-49d4721690bf", 2041432124);

            // UUID.
            map.Add(Guid.Parse("224ea4cd-f449-4dcb-869a-5317c63bd619"), -1767478264);
            map.Add(Guid.Parse("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd"), 1096337416);
            map.Add(Guid.Parse("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636"), 1269913698);
            map.Add(Guid.Parse("d8f1f916-4357-4cfe-a7df-49d4721690bf"), 1315925123);

            bool ok = true;

            foreach (var entry in map) {
                var actual = GridClientJavaHelper.GetJavaHashCode(entry.Key);

                if (entry.Value != actual) {
                    Dbg.WriteLine("Validation of hash code for '{0} {1}' failed [expected={2}, actual={3}.", entry.Key, entry.Key.GetType(), entry.Value, actual);

                    ok = false;
                }
            }

            if (ok)
                return;

            Assert.Fail("Java hash codes validation fails.");
        }
    }
}
