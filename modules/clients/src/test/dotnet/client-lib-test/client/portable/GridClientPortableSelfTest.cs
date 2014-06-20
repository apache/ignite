// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable {
    using System;
    using System.Collections.Generic;
    using System.Web.Script.Serialization;
    using NUnit.Framework;

    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;
    using GridGain.Client.Ssl;

    [TestFixture]
    public class GridClientPortableSelfTest : GridClientAbstractTest {

        private GridClientPortableMarshaller marsh;

        [TestFixtureSetUp]
        override public void InitClient()
        {
            marsh = new GridClientPortableMarshaller(null);
        }

        [TestFixtureTearDown]
        override public void StopClient()
        {
           // No-op.
        }

        public void TestPrimitiveInt()
        {
            int val = 1;

            marsh.Marshal(val, new GridClientPortableSerializationContext());
        }
    }
}
