using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks.Dataflow;
using Database.Core;
using Database.Core.DataStructures.Trees.RedBlack;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal class RedBlackNonUniqueNonClusteredIndex<TIndexKey, TPrimaryKey, TRow> : INonUniqueIndex<TIndexKey, TRow>, IIndexManagerItem<TIndexKey, TPrimaryKey, TRow>
    {
        private readonly AnonymousComparer<DataPage<TPrimaryKey, TRow>, TPrimaryKey> _primaryKeyComparer;
        private readonly Func<TRow, TIndexKey> _keySelector;
        
        private readonly RedBlackTreeMap<TIndexKey, Set<DataPage<TPrimaryKey, TRow>>> _map;

        public RedBlackNonUniqueNonClusteredIndex(Expression<Func<TRow, TIndexKey>> keySelector,
            IComparer<TIndexKey>? comparer, 
            bool allowNullKeys, 
            IComparer<TPrimaryKey> primaryKeyComparer)
        {
            _primaryKeyComparer = new AnonymousComparer<DataPage<TPrimaryKey, TRow>, TPrimaryKey>(k=>k.PrimaryKey, primaryKeyComparer);
            var selector = keySelector.Compile();
            _keySelector = allowNullKeys ? selector : KeySelector.ThrowOnNullKey(selector);
            _map = new RedBlackTreeMap<TIndexKey, Set<DataPage<TPrimaryKey, TRow>>>(comparer);
        }

        public void Add(TIndexKey key, DataPage<TPrimaryKey, TRow> page)
        {
            _map.AddOrReplace(key,
                () =>
                {
                    var group = new Set<DataPage<TPrimaryKey, TRow>>(_primaryKeyComparer);
                    group.Add(page);
                    return group;
                },
                s =>
                {
                    if(!s.Add(page))
                        throw new Exception($"Unable to add record to group at key: '{key}'. Row with primary key '{page.PrimaryKey}' already existed in the group");
                    return s;
                });
        }

        public void Remove(TIndexKey key, TPrimaryKey primaryKey)
        {
            if (!_map.TryGet(key, out var group))
                throw new Exception($"Unable to find group at key: '{key}'");
            
            if (!group.Value.Remove(DataPage<TPrimaryKey, TRow>.SurrogateForKey(primaryKey), out _))
                throw new Exception($"Attempted to remove non-existent row with primary key: '{primaryKey}' from group at key '{key}'");

            _map.Remove(key, out _);
        }

        public TIndexKey CalculateKey(TRow row) => _keySelector(row);
        public IComparer<TIndexKey> KeyComparer => _map.KeyComparer;
        public void Clear() => _map.Clear();

        public IEnumerable<IGrouping<TIndexKey, TRow>> GroupScan(Func<TIndexKey, bool> predicate, Retrieval retrieval)
            => _map.GetItems(retrieval.Reverse)
                .Where(d => predicate(d.Key))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(r=>new RedBlackNonUniqueNonClusteredGrouping<TPrimaryKey, TIndexKey, TRow>(r.Key, r.Value));

        
        public IEnumerable<TRow> Scan(Func<TIndexKey, bool> predicate, Retrieval retrieval) 
            => _map.GetItems(retrieval.Reverse)
                .Where(d => predicate(d.Key))
                .SelectMany(s => s.Value.GetItems(false))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(s=>s.Row);
       
        public IEnumerable<TRow> Seek(SeekTarget<TIndexKey> seekTarget, Retrieval retrieval)
        {
            seekTarget.Prepare(_map.KeyComparer, retrieval);

            return QueryAnalyser.Pick(seekTarget, _map.Count, 
                () => TrueSeek(seekTarget),
                () => RangedScan(seekTarget, retrieval.Reverse));
        }
        
        private IEnumerable<TRow> TrueSeek(SeekTarget<TIndexKey> seekTarget) 
            => _map.SeekMany(seekTarget.Keys!)
                .SelectMany(s => s.Value.GetItems(false))
                .Select(s=>s.Row);

        private IEnumerable<TRow> RangedScan(SeekTarget<TIndexKey> seekTarget, bool reverse)
        {
            return _map.Range(seekTarget.Min, seekTarget.Max, reverse)
                .Where(d => seekTarget.Contains(d.Key))
                .SelectMany(group => group.Value.GetItems(false)
                .Select(r=>r.Row));
        }
    }
}