// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    /** <summary>Null-key allowed dictionary tests.</summary> */
    [TestFixture]
    public class NullDictionaryTest {
        [Test]
        public void TestNullDictionary() {
            String[] keys = new String[] { null, "", "1", "asdf" };

            IDictionary<String, String> map = new GridClientNullDictionary<String, String>();

            // Fill the map (variant #1).
            foreach (var key in keys)
                map.Add(key, key);

            foreach (var key in keys) {
                Assert.IsTrue(map.Keys.Contains(key));
                Assert.IsTrue(map.Values.Contains(key));
                Assert.IsTrue(map.ContainsKey(key));
                Assert.IsTrue(map.Contains(new KeyValuePair<String, String>(key, key)));

                String val;

                // Try get value.
                map.TryGetValue(key, out val);

                Assert.AreEqual(key, val);
            }

            Assert.AreEqual(keys.Length, map.Count);
            Assert.AreEqual(keys.Length, map.Keys.Count);
            Assert.AreEqual(keys.Length, map.Values.Count);

            // Clear the map.
            map.Clear();

            Assert.AreEqual(0, map.Count);
            Assert.AreEqual(0, map.Keys.Count);
            Assert.AreEqual(0, map.Values.Count);
            Assert.IsFalse(map.GetEnumerator().MoveNext());
            Assert.IsFalse(((IEnumerable)map).GetEnumerator().MoveNext());

            // Fill the map (variant #2).
            foreach (var key in keys)
                map[key] = key;

            Assert.IsTrue(keys.Length == map.Count);

            foreach (var key in keys) {
                Assert.IsTrue(map.ContainsKey(key));
                Assert.IsTrue(map.Contains(new KeyValuePair<String, String>(key, key)));
                Assert.AreEqual(key, map[key]);
            }

            // Validate enumerators.
            IList<String> copy = new List<String>(keys);

            foreach (KeyValuePair<String, String> pair in map) {
                Assert.IsTrue(keys.Contains(pair.Key));

                copy.Remove(pair.Key);
            }

            Assert.AreEqual(0, copy.Count);

            copy = new List<String>(keys);

            foreach (object pair in (System.Collections.IEnumerable)map) {
                String key = ((KeyValuePair<String, String>)pair).Key;

                Assert.IsTrue(keys.Contains(key));

                copy.Remove(key);
            }

            Assert.AreEqual(0, copy.Count);
            Assert.AreEqual(map.Count, keys.Length);

            // Validate remove operation.
            for (int i = 0; i < keys.Length; i++) {
                Assert.AreEqual(map.Count, keys.Length - i);

                for (int j = 0; j < keys.Length; j++) {
                    Assert.AreEqual(j >= i, map.ContainsKey(keys[j]));
                    Assert.AreEqual(j >= i, map.Contains(new KeyValuePair<String, String>(keys[j], keys[j])));
                }

                map.Remove(keys[i]);

                Assert.AreEqual(map.Count, keys.Length - 1 - i);

                for (int j = 0; j < keys.Length; j++) {
                    Assert.AreEqual(j > i, map.ContainsKey(keys[j]));
                    Assert.AreEqual(j > i, map.Contains(new KeyValuePair<String, String>(keys[j], keys[j])));
                }
            }
        }
    }
}
