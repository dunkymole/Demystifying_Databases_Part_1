using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Disposables;
using Database.Core;
using Database.Core.DataStructures.Maybes;
using Database.Core.Indicies;
using Database.Interactive.Indicies;

namespace Database.Interactive
{
    public partial class Table<TPrimaryKey, TRow>
    {
        private readonly IClusteredIndex<TPrimaryKey, TRow> _clusteredIndex;
        private readonly IndexManager<TPrimaryKey, TRow> _indexManager;
        private readonly ConstraintManager<TPrimaryKey, TRow> _constraintManager = new ConstraintManager<TPrimaryKey,TRow>();
        
        internal Table(Expression<Func<TRow, TPrimaryKey>> keySelector, IComparer<TPrimaryKey>? comparer = null)
        {
            _clusteredIndex = new RedBlackClusteredIndex<TPrimaryKey, TRow>(keySelector, comparer);
            _indexManager = new IndexManager<TPrimaryKey, TRow>(keySelector, _clusteredIndex);
        }

        public int RowCount => _clusteredIndex.Count;

        public void BulkInsert(IEnumerable<TRow> rows)
        {
            _indexManager.PrepareBulkInsert();

            foreach (var row in rows)
            {
                _constraintManager.BeforeInsertUpdate(row); //you really should disable CheckConstraints before a bulk insert
                _clusteredIndex.Insert(row);
            }

            _indexManager.ResumeAfterBulkInsert();
        }

        public void Insert(TRow row)
        {
            _constraintManager.BeforeInsertUpdate(row);
            
            var page = _clusteredIndex.Insert(row);
            
            _indexManager.PropagateInsert(page);
        }

        public void Delete(TPrimaryKey key)
        {
            _constraintManager.BeforeDelete(key);
            
            var page = _clusteredIndex.Delete(key);
            
            _indexManager.PropagateDelete(page);
        }

        public void Update(TRow row)
        {
            _constraintManager.BeforeInsertUpdate(row);
            
            var page = _clusteredIndex.Update(row);
            
            _indexManager.PropagateUpdate(page);
        }

        public void Upsert(TPrimaryKey key, Func<IMaybe<TRow>, TRow> upsert)
        {
            var result = _clusteredIndex.Upsert(key,
                () => upsert(Maybe.Empty<TRow>()),
                v => upsert(Maybe.Return(v)),
                _constraintManager.BeforeInsertUpdate);

            if (result.PreviousValue.HasValue)
                _indexManager.PropagateUpdate((result.NewValue, result.PreviousValue.Value));
            else
                _indexManager.PropagateInsert(result.NewValue);
        }

        //Unique constraints aren't actually implemented constraints! 
        public void AddUniqueConstraint<TUnique>(
            Expression<Func<TRow, TUnique>> keySelector,
            IComparer<TUnique>? comparer = null,
            bool allowNullKeys = false) =>
            CreateIndex(keySelector, comparer, true, allowNullKeys);

        public void CreateIndex<TIndexKey>(
            Expression<Func<TRow, TIndexKey>> keySelector,
            IComparer<TIndexKey>? comparer = null,
            bool isUnique = false,
            bool allowNullKeys = false)
        {
            IIndexManagerItem<TIndexKey, TPrimaryKey, TRow> index;
            if (isUnique)
            {
                index = new RedBlackUniqueNonClusteredIndex<TIndexKey, TPrimaryKey, TRow>(keySelector, comparer, allowNullKeys);
            }
            else
            {
                index = new RedBlackNonUniqueNonClusteredIndex<TIndexKey, TPrimaryKey, TRow>(keySelector, comparer, allowNullKeys, _clusteredIndex.Comparer);
            }
            
            _indexManager.RegisterIndex(keySelector, index);
        }

        public void CreateFullTextIndex(Expression<Func<TRow, string>> keySelector, IEqualityComparer<string>? stringComparer = null)
        {
            var comparer = stringComparer ?? StringComparer.Ordinal;
            _indexManager.RegisterIndex(keySelector, new InvertedIndex<TPrimaryKey, TRow>(keySelector, comparer, _clusteredIndex.Comparer));
        }

        public void AddForeignKeyConstraint<TForeignTableKey, TForeignTable>(
            Table<TForeignTableKey, TForeignTable> foreignTable,
            Expression<Func<TRow, TForeignTableKey>> foreignKeySelector)
        {
            var ourKeySelector = foreignKeySelector.Compile();
            
            _constraintManager.RegisterInsertUpdateConstraint(ourRow =>
            {
                var key = ourKeySelector(ourRow);
                
                var foreignRecordsAtOurKey = foreignTable._clusteredIndex.Seek(SeekTarget.From(key), Retrieval.Default);
                
                if(!foreignRecordsAtOurKey.Any())
                    throw new Exception($"Key '{key}' does not exist in foreign table");
            });
            
            CreateIndex(foreignKeySelector);
            
            foreignTable._constraintManager.RegisterDeleteConstraint(theirPrimaryKey =>
            {
                var index = _indexManager.FindIndex<TForeignTableKey, IIndex<TForeignTableKey, TRow>>(foreignKeySelector).Value;
                var ourRecordsAtTheirKey = index.Seek(SeekTarget.From(theirPrimaryKey), Retrieval.Default);
                if(ourRecordsAtTheirKey.Any())
                    throw new Exception($"Can't delete '{theirPrimaryKey}' as a foreign table refers to it");
            });
        }

        public bool CheckConstraints
        {
            get => _constraintManager.Enabled;
            set => _constraintManager.Enabled = value;
        }

        private IDisposable GetOrCreateIndex<TIndexKey>(Expression<Func<TRow, TIndexKey>> keySelector, out IIndex<TIndexKey, TRow> index)
            => GetOrCreateIndexImpl(keySelector, out index);

        private IDisposable GetOrCreateNonUniqueIndex<TIndexKey>(Expression<Func<TRow, TIndexKey>> keySelector, out INonUniqueIndex<TIndexKey, TRow> index)
            => GetOrCreateIndexImpl(keySelector, out index);
                
        private IDisposable GetOrCreateIndexImpl<TIndexKey, TIndex>(Expression<Func<TRow, TIndexKey>> keySelector, out TIndex index) where TIndex : IIndex<TIndexKey, TRow>
        {
            var found = _indexManager.FindIndex<TIndexKey, TIndex>(keySelector);
            if (found.HasValue)
            {
                index = found.Value;
                return Disposable.Empty;
            }
            CreateIndex(keySelector);
            found = _indexManager.FindIndex<TIndexKey, TIndex>(keySelector);
            index = found.Value;
            return Disposable.Create(found.Value, i=>_indexManager.RemoveIndex(i));
        }
    }
}