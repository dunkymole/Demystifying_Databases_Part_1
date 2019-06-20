using System;

namespace Database.Core
{
    public static class KeySelector
    {
        public static Func<TRow, TKey> ThrowOnNullKey<TRow, TKey>(Func<TRow, TKey> keySelector) =>
            key =>
            {
                var k = keySelector(key);
                if(k == null) 
                    throw new ArgumentException($"Key cannot be null");
                return k;
            };
    }
}