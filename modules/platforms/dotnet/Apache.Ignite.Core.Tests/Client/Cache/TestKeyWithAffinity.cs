namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using Apache.Ignite.Core.Cache.Affinity;

    public sealed class TestKeyWithAffinity
    {
        [AffinityKeyMapped]
        private readonly int _i;

        private readonly string _s;

        public TestKeyWithAffinity(int i, string s)
        {
            _i = i;
            _s = s;
        }

        private bool Equals(TestKeyWithAffinity other)
        {
            return _i == other._i && string.Equals(_s, other._s);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TestKeyWithAffinity) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (_i * 397) ^ (_s != null ? _s.GetHashCode() : 0);
            }
        }
    }
}
