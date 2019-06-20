using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Database.Core;
using Database.Core.DataStructures.Trees.RedBlack;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal class RedBlackUniqueNonClusteredIndex<TIndexKey, TPrimaryKey, TRow> : IIndex<TIndexKey, TRow>, IIndexManagerItem<TIndexKey, TPrimaryKey, TRow>
    {
        private readonly Func<TRow, TIndexKey> _keySelector;
        private readonly RedBlackTreeMap<TIndexKey, DataPage<TPrimaryKey, TRow>> _map;

        public RedBlackUniqueNonClusteredIndex(Expression<Func<TRow, TIndexKey>> keySelector,
            IComparer<TIndexKey>? comparer, bool allowNullKeys)
        {
            var selector = keySelector.Compile();
            _keySelector = allowNullKeys ? selector : KeySelector.ThrowOnNullKey(selector);
            _map = new RedBlackTreeMap<TIndexKey, DataPage<TPrimaryKey, TRow>>(comparer);
        }

        public void Add(TIndexKey key, DataPage<TPrimaryKey, TRow> page)
        {
            if(!_map.Add(key, page))
                throw new Exception($"Unique index violation at key: '{key}'");
        }

        public void Remove(TIndexKey key, TPrimaryKey __)
        {
            if(!_map.Remove(key, out _))
                throw new Exception($"Attempted to remove non-existent key: '{key}'");
        }

        public TIndexKey CalculateKey(TRow row) => _keySelector(row);
        public IComparer<TIndexKey> KeyComparer => _map.KeyComparer;
        public void Clear() => _map.Clear();

        public IEnumerable<TRow> Scan(Func<TIndexKey, bool> predicate, Retrieval retrieval)
            => _map.GetItems(retrieval.Reverse)
                .Where(d => predicate(d.Key))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(s => s.Value.Row);

        public IEnumerable<TRow> Seek(SeekTarget<TIndexKey> seekTarget, Retrieval retrieval)
        {
            seekTarget.Prepare(_map.KeyComparer, retrieval);

            return QueryAnalyser.Pick(seekTarget, _map.Count, 
                () => _map.SeekMany(seekTarget.Keys!).Select(s=>s.Value.Row),
                () => RangedScan(seekTarget, retrieval.Reverse));
        }

        private IEnumerable<TRow> RangedScan(SeekTarget<TIndexKey> seekTarget, bool reverse)
        {
            return _map.Range(seekTarget.Min, seekTarget.Max, reverse)
                .Where(d => seekTarget.Contains(d.Key))
                .Select(d => d.Value.Row);
        }
    }
}