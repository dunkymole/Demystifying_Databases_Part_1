using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Database.Core.DataStructures.Trees.RedBlack;

namespace Database.Interactive.Indicies
{
    internal class RedBlackNonUniqueNonClusteredGrouping<TPrimaryKey, TIndexKey, TRow> : IGrouping<TIndexKey, TRow>
    {
        private readonly List<TRow> _items;

        public RedBlackNonUniqueNonClusteredGrouping(TIndexKey key, Set<DataPage<TPrimaryKey, TRow>> groupItems)
        {
            Key = key;
            _items = groupItems.GetItems(false).Select(dp=>dp.Row).ToList();
        }

        public IEnumerator<TRow> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public TIndexKey Key { get; }
    }
}