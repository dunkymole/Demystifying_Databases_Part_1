using System;
using System.Collections.Generic;
using System.Linq;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal interface INonUniqueIndex<TKey, TRow> : IIndex<TKey, TRow>
    {
        IEnumerable<IGrouping<TKey, TRow>> GroupScan(Func<TKey, bool> predicate, Retrieval retrieval);
    }
}