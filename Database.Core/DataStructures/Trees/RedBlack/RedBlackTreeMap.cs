using System;
using System.Collections.Generic;

namespace Database.Core.DataStructures.Trees.RedBlack
{
    public class RedBlackTreeMap<TKey, TValue>
    {
        private readonly AnonymousComparer<(TKey Key, TValue Value), TKey> _keyValueComparer;
        private readonly Set<(TKey Key, TValue Value)> _tree;

        public RedBlackTreeMap(IComparer<TKey>? keyComparer = null)
        {
            _keyValueComparer = new AnonymousComparer<(TKey Key, TValue Value), TKey>(kv=>kv.Key, keyComparer);
            _tree = new Set<(TKey Key, TValue Value)>(_keyValueComparer);
        }

        public int Count => _tree.Count;
        public IComparer<TKey> KeyComparer => _keyValueComparer.KeyComparer;
        public int Version => _tree.Version;
        
        public IEnumerable<(TKey Key, TValue Value)> GetItems(bool reverse) 
            => _tree.GetItems(reverse);

        public IEnumerable<(TKey Key, TValue Value)> Range(TKey from, TKey to, bool reverse) 
            => _tree.Range((from, default!), (to, default!), reverse);
        
        public void Clear() => _tree.Clear();

        public bool Add(TKey key, TValue value) => _tree.Add((key, value));

        public bool Remove(TKey key, out TValue removed)
        {
            if (_tree.Remove((key, default!), out var found))
            {
                removed = found.Value;
                return true;
            }
            removed = default!;
            return false;
        }

        public bool TryGet(TKey key, out (TKey Key, TValue Value) value) 
        {
            if (_tree.TryGetValue((key, default!), out var found))
            {
                value = found;
                return true;
            }
            value = default!;
            return false;
        }
        
        public TValue AddOrReplace(TKey key, Func<TValue> factoryFunction, Func<TValue, TValue> existedFunction) =>
            _tree.AddOrReplace((key, default!),
                    () => (key, factoryFunction()), 
                    existing => (key, existedFunction(existing.Value)))
            .Value;
    }

    public static class RedBlackTreeMapExtensions
    {
        public static IEnumerable<(TKey Key, TItem Value)> SeekMany<TKey, TItem>(this RedBlackTreeMap<TKey, TItem> source, IEnumerable<TKey> keys)
        {
            foreach (var key in keys)
                if (source.TryGet(key, out var found))
                    yield return found;
        }
    }
}