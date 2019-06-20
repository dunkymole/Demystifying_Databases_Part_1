using System;
using System.Collections.Generic;
using Database.Core.DataStructures.Maybes;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal interface IClusteredIndex<TPrimaryKey, TRow> : IIndex<TPrimaryKey, TRow>
    {
        IEnumerable<DataPage<TPrimaryKey, TRow>> GetAllPages();
        IComparer<TPrimaryKey> Comparer { get; }
        int Count { get; }
        DataPage<TPrimaryKey, TRow> Insert(TRow value);
        DataPage<TPrimaryKey, TRow> Delete(TPrimaryKey key);
        (DataPage<TPrimaryKey, TRow> NewValue, TRow PreviousValue) Update(TRow newValue);
        (DataPage<TPrimaryKey, TRow> NewValue, IMaybe<TRow> PreviousValue) Upsert(
            TPrimaryKey key, 
            Func<TRow> factoryFunction, 
            Func<TRow, TRow> updateFunction,
            Action<TRow> onBeforeUpsert);
    }
}