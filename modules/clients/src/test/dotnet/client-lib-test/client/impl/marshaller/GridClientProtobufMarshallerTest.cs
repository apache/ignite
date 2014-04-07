// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller {
    using System;
    using sc = System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    using GridGain.Client.Util;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Impl.Protobuf;

    using GridCacheOp = GridGain.Client.Impl.Message.GridClientCacheRequestOperation;
    using ProtoCacheOp = GridGain.Client.Impl.Protobuf.ProtoCacheRequest.Types.GridCacheOperation;

    using Dbg = System.Diagnostics.Debug;

    [TestFixture]
    public class GridClientProtobufMarshallerTest {
        private readonly GridClientProtobufMarshaller m = new GridClientProtobufMarshaller();

        [Test]
        public void TestCacheOpsConvertions() {
            /* Cache operations convertions. */
            var cacheOps = new Dictionary<ProtoCacheOp, GridCacheOp>();

            /* Fill cache operations convertions. */
            cacheOps[ProtoCacheOp.PUT] = GridCacheOp.Put;
            cacheOps[ProtoCacheOp.PUT_ALL] = GridCacheOp.PutAll;
            cacheOps[ProtoCacheOp.GET] = GridCacheOp.Get;
            cacheOps[ProtoCacheOp.GET_ALL] = GridCacheOp.GetAll;
            cacheOps[ProtoCacheOp.RMV] = GridCacheOp.Rmv;
            cacheOps[ProtoCacheOp.RMV_ALL] = GridCacheOp.RmvAll;
            cacheOps[ProtoCacheOp.REPLACE] = GridCacheOp.Replace;
            cacheOps[ProtoCacheOp.CAS] = GridCacheOp.Cas;
            cacheOps[ProtoCacheOp.METRICS] = GridCacheOp.Metrics;
            cacheOps[ProtoCacheOp.APPEND] = GridCacheOp.Append;
            cacheOps[ProtoCacheOp.PREPEND] = GridCacheOp.Prepend;

            /* Cache operations back convertions. */
            var cacheOpsInv = new Dictionary<GridCacheOp, ProtoCacheOp>();

            /* Fill cache operations back convertions. */
            foreach (KeyValuePair<ProtoCacheOp, GridCacheOp> pair in cacheOps)
                cacheOpsInv[pair.Value] = pair.Key;

            /* Validate all proto cache operations are mapped to grid client operations. */
            foreach (ProtoCacheOp op in Enum.GetValues(typeof(ProtoCacheOp)))
                Assert.AreEqual((int)op, (int)cacheOps[op]);

            /* Validate all grid client cache operations are mapped to proto operations. */
            foreach (GridCacheOp op in Enum.GetValues(typeof(GridCacheOp)))
                Assert.AreEqual((int)op, (int)cacheOpsInv[op]);
        }

        [Test]
        public void TestSimpleMarshalling() {
            // Null.
            Assert.IsNull(MU<Object>(null));

            // Boolean.
            Assert.IsTrue(MU(true));
            Assert.IsFalse(MU(false));

            // Byte.
            for (byte i = 0; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Integer 16.
            for (short i = -100; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Integer 32.
            for (int i = -100; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Integer 64.
            for (long i = -100; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Floating 32.
            for (float i = -100.5f; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Floating 64.
            for (double i = -100.5; i < 100; i++)
                Assert.AreEqual(i, MU(i));

            // Strings.
            String s = "asdf";

            for (int i = -100; i < 100; i++) {
                s += i;

                Assert.AreEqual(s, MU(s));
            }

            // Guids.
            for (int i = -100; i < 100; i++) {
                Guid uuid = Guid.NewGuid();

                Assert.AreEqual(uuid, MU(uuid));
            }

            // Collection.
            sc::IList list = new sc::ArrayList();

            for (int i = -100; i < 100; i++) {
                list.Add(i);

                Assert.AreEqual(list, MU(list));
            }

            // Map.
            sc::IDictionary map = new sc::Hashtable();

            for (int i = -100; i < 100; i++) {
                map.Add(i, i * i);

                Assert.AreEqual(map, MU<sc::IDictionary>(map));
            }
        }

        [Test]
        public void TestUuidMarshalling() {
            IList<String> list = new List<String>();

            list.Add("2ec84557-f7c4-4a2e-aea8-251eb13acff3");
            list.Add("4e17b7b5-79e7-4db5-ac45-a644ead95b9e");
            list.Add("412daadb-e9e6-443b-8b87-8d7895fc2e53");
            list.Add("e71aabf4-4aad-4280-b4e9-3c310be0cb88");
            list.Add("d4454cda-a81f-490f-9424-9bdfcc9cf610");
            list.Add("3a584450-5e85-4b69-9f9d-043d89fef23b");
            list.Add("6c8baaec-f173-4a60-b566-240a87d7f81d");
            list.Add("d99c7102-79f7-4fb4-a665-d331cf285c20");
            list.Add("007d56c7-5c8b-4279-a700-7f3f95946dde");
            list.Add("15627963-d8f9-4423-bedc-f6f89f7d3433");
            
            foreach (String str in list) {
                Guid uuid = Guid.Parse(str);

                GridClientAuthenticationRequest req1 = new GridClientAuthenticationRequest(Guid.NewGuid());

                req1.ClientId = uuid;
                req1.Credentials = "s3cret";

                GridClientAuthenticationRequest req2 = MU(req1);

                Assert.AreEqual(req1.Credentials, req2.Credentials);
                //Assert.AreEqual(req1.ClientId, req2.ClientId);
                //Assert.AreEqual(uuid, req2.ClientId);
                //Assert.AreEqual(Guid.Empty, req2.DestNodeId);
            }
        }


        /**
         * <summary>
         * Marshal-unmarshal sequence.</summary>
         *
         * <param name="value">Value to marshal and then unmarshal.</param>
         * <returns>Marshalled and then unmarshalled value.</returns>
         */
        private T MU<T>(T val) {
            return m.Unmarshal<T>(m.Marshal(val));
        }
    }
}
