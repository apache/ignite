namespace Apache.Ignite.Core.Tests.Client.Cache
{
    public sealed class TestKey
    {
        private readonly int _i;
        private readonly string _s;

        public TestKey(int i, string s)
        {
            _i = i;
            _s = s;
        }

        public int I
        {
            get { return _i; }
        }

        public string S
        {
            get { return _s; }
        }

        private bool Equals(TestKey other)
        {
            return _i == other._i && string.Equals(_s, other._s);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TestKey) obj);
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
