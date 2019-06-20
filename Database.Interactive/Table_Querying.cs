using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Database.Core.Indicies;

namespace Database.Interactive
{
    public partial class Table<TPrimaryKey, TRow>
    {
        private static bool PermissivePredicate<TPredicateKey>(TPredicateKey _) => true;
        
        //our predicate is defined in terms of the row itself, therefore no index can actually help us out
        public IEnumerable<TOut> QueryWithTableScan<TOut>(
            Func<TRow, TOut> select,
            Func<TRow, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            return _clusteredIndex.Scan(PermissivePredicate, new Retrieval(skip, take, reverse))
                .Where(where ?? PermissivePredicate)
                .Select(select)
                .ToArray()
                .AsEnumerable();
        }

        public IEnumerable<TOut> QueryWithIndexScan<TPredicateKey, TOut>(
            Expression<Func<TRow, TPredicateKey>> indexSelector,
            Func<TRow, TOut> select,
            Func<TPredicateKey, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            var index = _indexManager.FindIndex(indexSelector);
            if (!index.HasValue)
                throw new Exception($"No index registered for key: '{Expressions.GetMemberName(indexSelector)}'");

            return index.Value.Scan(where ?? PermissivePredicate, new Retrieval(skip, take, reverse))
                .Select(select)
                .ToArray()
                .AsEnumerable();
        }

        public IEnumerable<TOut> QueryWithIndexSeek<TPredicateKey, TOut>(
            Expression<Func<TRow, TPredicateKey>> indexSelector,
            Func<TRow, TOut> select,
            IEnumerable<TPredicateKey> fromKeys,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            var index = _indexManager.FindIndex(indexSelector);
            if (!index.HasValue)
                throw new Exception($"No index registered for key: '{Expressions.GetMemberName(indexSelector)}'");

            return index.Value.Seek(SeekTarget.From(fromKeys), new Retrieval(skip, take, reverse))
                .Select(select)
                .ToArray()
                .AsEnumerable();
        }
        
        public IEnumerable<TOut> QueryWithIndexSeek<TPredicateKey, TOut>(
            Expression<Func<TRow, TPredicateKey>> indexSelector,
            Func<TRow, TOut> select,
            IEnumerable<TPredicateKey?> fromKeys,
            int? skip = null,
            int? take = null,
            bool reverse = false) where TPredicateKey : struct
        {
            return QueryWithIndexSeek(
                indexSelector,
                select,
                fromKeys.Where(k=>k.HasValue).Select(k=>k!.Value),
                skip,
                take,
                reverse);
        }

        public IEnumerable<(TGrouping, TAggregate)> GroupBy<TGrouping, TAggregate, TAccumalator>(
            Expression<Func<TRow, TGrouping>> groupingSelector,
            Func<TAccumalator> accumalatorFactory,
            Func<TAccumalator, TRow, TAccumalator> accumalate,
            Func<TAccumalator, TAggregate> accumalateProjection,
            Func<TGrouping, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            var predicate = where ?? PermissivePredicate;
            using var _ = GetOrCreateNonUniqueIndex(groupingSelector, out var index);
            return index.GroupScan(predicate, new Retrieval(skip, take, reverse))
                .Select(group => (group.Key, group.Aggregate(accumalatorFactory(), accumalate, accumalateProjection)))
                .ToArray()
                .AsEnumerable();
        }
        
        public IEnumerable<(TGrouping Key, int Sum)> GroupByWithCount<TGrouping>(
            Expression<Func<TRow, TGrouping>> groupingSelector,
            Func<TGrouping, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            return GroupBy(groupingSelector,
                () => 0,
                (acc, row) => acc + 1,
                acc => acc,
                where,
                skip,
                take,
                reverse);
        }

        public IEnumerable<(TGrouping Key, int Sum)> GroupByWithSum<TGrouping>(
            Expression<Func<TRow, TGrouping>> groupingSelector,
            Func<TRow, int> toSumSelector,
            Func<TGrouping, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            return GroupBy(groupingSelector,
                () => 0,
                (acc, row) => acc + toSumSelector(row),
                acc => acc,
                where,
                skip,
                take,
                reverse);
        }

        public IEnumerable<(TGrouping Key, double Mean)> GroupByWithAverage<TGrouping>(
            Expression<Func<TRow, TGrouping>> groupingSelector,
            Func<TRow, int> toAverageSelector,
            Func<TGrouping, bool>? where = null,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            return GroupBy(groupingSelector,
                () => (Count: 0, Sum: 0),
                (acc, row) => (acc.Count + 1, acc.Sum + toAverageSelector(row)),
                acc => acc.Sum / (double) acc.Count,
                where,
                skip,
                take,
                reverse);
        }

        //implements a "temporary index nested loops join" (or index assisted if the index exists already)
        //https://docs.microsoft.com/en-us/sql/relational-databases/performance/joins?view=sql-server-2017#nested_loops
        public IEnumerable<TJoined> InnerJoin<TOtherTableKey, TOtherTableRow, TJoined, TJoinKey>(
            Table<TOtherTableKey, TOtherTableRow> rightTable,
            Expression<Func<TRow, TJoinKey>> leftTableJoinKeySelector,
            Expression<Func<TOtherTableRow, TJoinKey>> rightTableJoinKeySelector,
            Func<TRow, TOtherTableRow, TJoined> select,
            Func<TJoined, bool>? where = null)
        {
            var leftIsSmaller = RowCount < rightTable.RowCount;

            var arbitarySeekCacheSizeLimit = 10_000;
            
            if (leftIsSmaller)
            {
                var rightKeySelector = rightTableJoinKeySelector.Compile();

                //build
                using var __ = GetOrCreateIndex(leftTableJoinKeySelector, out var leftIndex);

                Func<TJoinKey, IEnumerable<TRow>> leftSeek = key => leftIndex.Seek(SeekTarget.From(key), Retrieval.Default);
                var leftSeekFast = leftSeek.Memoise(arbitarySeekCacheSizeLimit);

                //probe
                var q = from outer in rightTable._clusteredIndex.Scan(PermissivePredicate, Retrieval.Default)
                    let key = rightKeySelector(outer)
                    from inner in leftSeekFast(key)
                    select @select(inner, outer);

                return q.Where(where ?? PermissivePredicate).ToArray().AsEnumerable();
            }

            var leftKeySelector = leftTableJoinKeySelector.Compile();

            //build
            using var ___ = rightTable.GetOrCreateIndex(rightTableJoinKeySelector, out var rightIndex);

            Func<TJoinKey, IEnumerable<TOtherTableRow>> rightSeek = key => rightIndex.Seek(SeekTarget.From(key), Retrieval.Default);
            var rightSeekFast = rightSeek.Memoise(arbitarySeekCacheSizeLimit);

            //probe
            var q1 = from outer in _clusteredIndex.Scan(PermissivePredicate, Retrieval.Default)
                let key = leftKeySelector(outer)
                from inner in rightSeekFast(key)
                select @select(outer, inner);

            return q1.Where(where ?? PermissivePredicate).ToArray().AsEnumerable();
        }

        public IEnumerable<TRow> QueryWithFullTextIndex(
            Expression<Func<TRow, string>> predicateSelector,
            string searchText,
            int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            var index = _indexManager.FindInvertedIndex(predicateSelector);
            if (!index.HasValue)
                throw new Exception(
                    $"Could not find an inverted index for: '{Expressions.GetMemberName(predicateSelector)}'");

            return index.Value.Seek(searchText, new Retrieval(skip, take, reverse))
                .ToArray()
                .AsEnumerable();
        }
    }
}