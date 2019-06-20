using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Database.Core;
using Database.Core.DataStructures.Maybes;
using Database.Core.DataStructures.Trees.RedBlack;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal class RedBlackClusteredIndex<TPrimaryKey, TRow> : IClusteredIndex<TPrimaryKey, TRow>
    {
        private readonly RedBlackTreeMap<TPrimaryKey, DataPage<TPrimaryKey, TRow>> _map;
        private readonly Func<TRow, TPrimaryKey> _keySelector;

        public RedBlackClusteredIndex(Expression<Func<TRow, TPrimaryKey>> keySelector, IComparer<TPrimaryKey>? comparer)
        {
            _keySelector = KeySelector.ThrowOnNullKey(keySelector.Compile());
            _map = new RedBlackTreeMap<TPrimaryKey, DataPage<TPrimaryKey, TRow>>(comparer);
        }

        public IEnumerable<DataPage<TPrimaryKey, TRow>> GetAllPages() => _map.GetItems(false).Select(s=>s.Value);

        public IComparer<TPrimaryKey> Comparer => _map.KeyComparer;
        public int Count => _map.Count;

        public DataPage<TPrimaryKey, TRow> Insert(TRow value)
        {
            var key = _keySelector(value);
            var page = new DataPage<TPrimaryKey, TRow>(key, value);
            if(_map.Add(key, page))
                return page;
            
            throw new Exception($"Record existed at key: '{key}'");
        }

        public DataPage<TPrimaryKey, TRow> Delete(TPrimaryKey key)
        {
            if(_map.Remove(key, out var removed))
                return removed;
            
            throw new Exception($"Nothing to delete at at key: '{key}'");
        }

        public (DataPage<TPrimaryKey, TRow> NewValue, TRow PreviousValue) Update(TRow newValue)
        {
            var key = _keySelector(newValue);
            if (!_map.TryGet(key, out var page))
                throw new Exception($"Failed to row to update at key: '{key}'");
            
            var previous = page.Value.Row;
            page.Value.Update(newValue);
            return (page.Value, previous);
        }

        public (DataPage<TPrimaryKey, TRow> NewValue, IMaybe<TRow> PreviousValue) Upsert(
            TPrimaryKey key, 
            Func<TRow> factoryFunction, 
            Func<TRow, TRow> updateFunction,
            Action<TRow> onBeforeUpsert)
        {
            var previous = Maybe.Empty<TRow>();
            
            var newValue = _map.AddOrReplace(key,
                () =>
                {
                    var toInsert = factoryFunction();
                    onBeforeUpsert?.Invoke(toInsert);
                    return new DataPage<TPrimaryKey, TRow>(key, toInsert);
                },
                p =>
                {
                    previous = Maybe.Return(p.Row);
                    var toUpdate = updateFunction(previous.Value);
                    onBeforeUpsert?.Invoke(toUpdate);
                    p.Update(toUpdate);
                    return p;
                });
            return (newValue, previous);
        }
        
        public IEnumerable<TRow> TableScan(Func<TRow, bool> predicate, Retrieval retrieval) =>
            _map.GetItems(retrieval.Reverse)
                .Where(r=>predicate(r.Value.Row))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(s => s.Value.Row);

        public IEnumerable<TRow> Scan(Func<TPrimaryKey, bool> predicate, Retrieval retrieval) 
            => _map.GetItems(retrieval.Reverse)
                .Where(d => predicate(d.Key))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(s => s.Value.Row);

        public IEnumerable<TRow> Seek(SeekTarget<TPrimaryKey> seekTarget, Retrieval retrieval)
        {
            seekTarget.Prepare(Comparer, retrieval);
            
            return QueryAnalyser.Pick(seekTarget, _map.Count, 
                () => _map.SeekMany(seekTarget.Keys!).Select(s=>s.Value.Row),
                () => RangedScan(seekTarget, retrieval.Reverse));
        }

        private IEnumerable<TRow> RangedScan(SeekTarget<TPrimaryKey> seekTarget, bool reverse)
        {
            return _map.Range(seekTarget.Min,  seekTarget.Max, reverse)
                .Where(d => seekTarget.Contains(d.Key))
                .Select(d => d.Value.Row);
        }
    }
}