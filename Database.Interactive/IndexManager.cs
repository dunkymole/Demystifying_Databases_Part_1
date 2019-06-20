using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Database.Core.DataStructures.Maybes;
using Database.Interactive.Indicies;

namespace Database.Interactive
{
    internal class IndexManager<TPrimaryKey, TRow>
    {
        private readonly IClusteredIndex<TPrimaryKey, TRow> _clusteredIndex;
        private readonly List<(string Name, IndexManagerEntry<TPrimaryKey, TRow> IndexEntry)> _indices = new List<(string, IndexManagerEntry<TPrimaryKey, TRow>)>();
        private readonly string _primaryKeyColumnName;

        public IndexManager(Expression<Func<TRow, TPrimaryKey>> primaryKeySelector, IClusteredIndex<TPrimaryKey, TRow> clusteredIndex)
        {
            _primaryKeyColumnName = Expressions.GetMemberName(primaryKeySelector);
            _clusteredIndex = clusteredIndex;
        }

        public void PropagateInsert(DataPage<TPrimaryKey,TRow> page)
        {
            foreach (var (_, indexEntry) in SafeGetIndicies()) 
                indexEntry.PropagateInsert(page);
        }

        public void PropagateDelete(DataPage<TPrimaryKey,TRow> page)
        {
            foreach (var (_, indexEntry) in SafeGetIndicies()) 
                indexEntry.PropagateDelete(page);
        }

        public void PropagateUpdate((DataPage<TPrimaryKey,TRow> NewValue, TRow PriorValue) update)
        {
            foreach (var (_, indexEntry) in SafeGetIndicies()) 
                indexEntry.PropagateUpdate(update);
        }

        private IEnumerable<(string Name, IndexManagerEntry<TPrimaryKey, TRow> IndexEntry)> SafeGetIndicies()
        {
            lock (_indices)
                return _indices.ToArray().AsEnumerable();
        }
        
        public void RegisterIndex<TIndexKey>(Expression<Func<TRow, TIndexKey>> keySelector,
            IIndexManagerItem<TIndexKey, TPrimaryKey, TRow> indexManagerItem)
        {
            var columnName = Expressions.GetMemberName(keySelector);
            var indexEntry = IndexManagerEntry<TPrimaryKey, TRow>.FromIndex(indexManagerItem);

            foreach (var existingRecord in _clusteredIndex.GetAllPages()) 
                indexEntry.PropagateInsert(existingRecord);

            lock (_indices)
            {
                _indices.Add((columnName, indexEntry));    
            }
        }
        
        public void RemoveIndex(object indexToRemove)
        {
            lock (_indices)
            {
                var found = _indices.FirstOrDefault(entry => entry.IndexEntry.UnderlyingIndex == indexToRemove);
                if (found == default)
                    throw new Exception("Failed to find index to remove");
            
                _indices.Remove(found);
            }
        }

        public IMaybe<IIndex<TIndexKey, TRow>> FindIndex<TIndexKey>(Expression<Func<TRow, TIndexKey>> keySelector)
        {
            return FindIndex<TIndexKey, IIndex<TIndexKey, TRow>>(keySelector);
        }

        public IMaybe<INonUniqueIndex<TIndexKey, TRow>> FindNonUniqueIndex<TIndexKey>(Expression<Func<TRow, TIndexKey>> keySelector)
            => FindIndex<TIndexKey, INonUniqueIndex<TIndexKey, TRow>>(keySelector);
        
        public IMaybe<IInvertedIndex<TRow>> FindInvertedIndex(Expression<Func<TRow, string>> keySelector)
            => FindIndex<string, IInvertedIndex<TRow>>(keySelector);
        
        public IMaybe<TIndex> FindIndex<TIndexKey, TIndex>(Expression<Func<TRow, TIndexKey>> keySelector)
        {
            var desiredKey = Expressions.GetMemberName(keySelector);
            if (desiredKey == _primaryKeyColumnName && _clusteredIndex is TIndex primary) 
                return Maybe.Return(primary);
            
            lock (_indices)
            {
                foreach (var (coversKey, entry) in _indices.Select(d=>d))
                {
                    if (coversKey == desiredKey && entry.UnderlyingIndex is TIndex match)
                        return Maybe.Return(match);
                }

                return Maybe.Empty<TIndex>();
            }
        }


        public void PrepareBulkInsert()
        {
            foreach (var (_, indexEntry) in SafeGetIndicies()) 
                indexEntry.Clear();
        }

        public void ResumeAfterBulkInsert()
        {
            var newPages = _clusteredIndex.GetAllPages().ToArray();
            
            Parallel.ForEach(SafeGetIndicies(), index =>
            {
                foreach (var page in newPages)
                    index.IndexEntry.PropagateInsert(page);
            });
        }

    }
}