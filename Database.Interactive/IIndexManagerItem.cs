using System.Collections.Generic;

namespace Database.Interactive
{
    internal interface IIndexManagerItem<TKey, TPrimaryKey, TRow>
    {
        void Add(TKey key, DataPage<TPrimaryKey, TRow> page);
        void Remove(TKey key, TPrimaryKey primaryKey);
        TKey CalculateKey(TRow row);
        IComparer<TKey> KeyComparer { get; }
        void Clear();
    }
}