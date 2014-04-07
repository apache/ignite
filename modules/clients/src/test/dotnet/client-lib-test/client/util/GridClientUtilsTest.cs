// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    using U = GridGain.Client.Util.GridClientUtils;

    [TestFixture]
    public class GridClientUtilsTest {
        private const int TEST_CYCLE_SIZE = 1000;

        private readonly Random rnd = new Random();

        [Test]
        public void TestUuidConvertions() {
            // Java UUID in text and byte formats.
            IDictionary<String, sbyte[]> map = new Dictionary<String, sbyte[]>();

            map.Add("2ec84557-f7c4-4a2e-aea8-251eb13acff3", new sbyte[] {
                46, -56, 69, 87, -9, -60, 74, 46, -82, -88, 37, 30, -79, 58, -49, -13 });
            map.Add("4e17b7b5-79e7-4db5-ac45-a644ead95b9e", new sbyte[] {
                78, 23, -73, -75, 121, -25, 77, -75, -84, 69, -90, 68, -22, -39, 91, -98 });
            map.Add("412daadb-e9e6-443b-8b87-8d7895fc2e53", new sbyte[] {
                65, 45, -86, -37, -23, -26, 68, 59, -117, -121, -115, 120, -107, -4, 46, 83 });
            map.Add("e71aabf4-4aad-4280-b4e9-3c310be0cb88", new sbyte[] {
                -25, 26, -85, -12, 74, -83, 66, -128, -76, -23, 60, 49, 11, -32, -53, -120 });
            map.Add("d4454cda-a81f-490f-9424-9bdfcc9cf610", new sbyte[] {
                -44, 69, 76, -38, -88, 31, 73, 15, -108, 36, -101, -33, -52, -100, -10, 16 });
            map.Add("3a584450-5e85-4b69-9f9d-043d89fef23b", new sbyte[] {
                58, 88, 68, 80, 94, -123, 75, 105, -97, -99, 4, 61, -119, -2, -14, 59 });
            map.Add("6c8baaec-f173-4a60-b566-240a87d7f81d", new sbyte[] {
                108, -117, -86, -20, -15, 115, 74, 96, -75, 102, 36, 10, -121, -41, -8, 29 });
            map.Add("d99c7102-79f7-4fb4-a665-d331cf285c20", new sbyte[] {
                -39, -100, 113, 2, 121, -9, 79, -76, -90, 101, -45, 49, -49, 40, 92, 32 });
            map.Add("007d56c7-5c8b-4279-a700-7f3f95946dde", new sbyte[] {
                0, 125, 86, -57, 92, -117, 66, 121, -89, 0, 127, 63, -107, -108, 109, -34 });
            map.Add("15627963-d8f9-4423-bedc-f6f89f7d3433", new sbyte[] {
                21, 98, 121, 99, -40, -7, 68, 35, -66, -36, -10, -8, -97, 125, 52, 51 });

            foreach (KeyValuePair<String, sbyte[]> e in map) {
                Guid uuid = Guid.Parse(e.Key);

                byte[] copy = new byte[e.Value.Length];

                for (int i = 0, len = copy.Length; i < len; i++)
                    copy[i] = (byte)e.Value[i];

                Guid uuidFromBytes = GridClientUtils.BytesToGuid(copy, 0);

                Assert.AreEqual(uuid, uuidFromBytes);
                Assert.AreEqual(e.Key, uuid.ToString());
                Assert.AreEqual(e.Key, uuidFromBytes.ToString());

                byte[] bytes = GridClientUtils.ToBytes(uuid);

                for (int i = 0, len = Math.Max(copy.Length, bytes.Length); i < len; i++)
                    Assert.AreEqual(copy[i], bytes[i], e.Key);
            }
        }

        [Test]
        public void TestShortToBytes() {
            for (int i = 0; i < TEST_CYCLE_SIZE; i++) {
                byte[] a = GenerateArray(2);
                short v = (short)((a[0] << 8) + a[1]);

                Assert.AreEqual(a, U.ToBytes(v));
                Assert.AreEqual(v, U.BytesToInt16(a, 0));
            }
        }

        [Test]
        public void TestIntToBytes() {
            for (int i = 0; i < TEST_CYCLE_SIZE; i++) {
                byte[] a = GenerateArray(4);
                int v = (int)((a[0] << 24) + (a[1] << 16) + (a[2] << 8) + a[3]);

                Assert.AreEqual(a, U.ToBytes(v));
                Assert.AreEqual(v, U.BytesToInt32(a, 0));
            }
        }

        [Test]
        public void TestLongToBytes() {
            for (int i = 0; i < TEST_CYCLE_SIZE; i++) {
                byte[] a = GenerateArray(8);
                ulong v = 0;
                v += (uint)((a[0] << 24) + (a[1] << 16) + (a[2] << 8) + a[3]);
                v <<= 32;
                v += (uint)((a[4] << 24) + (a[5] << 16) + (a[6] << 8) + a[7]);

                Assert.AreEqual(a, U.ToBytes((long)v));
                Assert.AreEqual((long)v, U.BytesToInt64(a, 0));
                Assert.AreEqual(v, (ulong)U.BytesToInt64(a, 0));
            }
        }

        [Test]
        public void TestList() {
            IList<String> list = new List<String>();

            Assert.AreEqual(list, U.List<String>());

            try {
                U.List<String>(null);

                Assert.Fail("Should not create list from null value");
            }
            catch (ArgumentException) {
                /* Noop - normal behaviour */
            }

            list.Add(null);
            Assert.AreEqual(list, U.List((String)null));

            list.Add("");
            Assert.AreEqual(list, U.List(null, ""));

            list.Add("asdf");
            Assert.AreEqual(list, U.List(null, "", "asdf"));

            list.Add("fdsa");
            Assert.AreEqual(list, U.List<String>(null, "", "asdf", "fdsa"));
        }

        private byte[] GenerateArray(int size) {
            byte[] a = new byte[size];

            rnd.NextBytes(a);

            return a;
        }
    }
}
