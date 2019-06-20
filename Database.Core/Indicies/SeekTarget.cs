using System;
using System.Collections.Generic;
using System.Linq;

namespace Database.Core.Indicies
{
    public static class SeekTarget
    {
        public static SeekTarget<TKey> From<TKey>(TKey key)
            => new SeekTarget<TKey>(new[] {key});
        public static SeekTarget<TKey> From<TKey>(IEnumerable<TKey> keys)
            => new SeekTarget<TKey>(keys);
        
        public static SeekTarget<TKey> From<TKey>(params TKey[] keys)
            => new SeekTarget<TKey>(keys);
    }

    public class SeekTarget<TKey>
    {
        private readonly IEnumerable<TKey> _keys;
        private IComparer<TKey>? Comparer { get; set; }
        
        private bool _prepared;
        public TKey[]? Keys { get; private set; }
        internal SeekTarget(IEnumerable<TKey> keys)
        {
            if(!keys.Any())
                throw new ArgumentException("Empty key list");
            _keys = keys;
        }

        public void Prepare(IComparer<TKey> ambient, Retrieval retrieval)
        {
            if (_prepared) return;
            
            if (Comparer == null) 
                Comparer = ambient;
            
            Keys = _keys.OrderBy(s=>s, Comparer)
                    .Skip(retrieval.Skip ?? 0)
                    .Take(retrieval.Take ?? Int32.MaxValue)
                    .ToArray();
            
            _prepared = true;
        }

        public TKey Min => Keys![0];
        public TKey Max => Keys![Keys.Length -1];

        public bool Contains(TKey key) => Keys.Any(k => Comparer.Compare(k, key) == 0);
                                         //TODO profile vs               
                                         //Array.BinarySearch(Keys, key, Comparer) > -1;
    }
}