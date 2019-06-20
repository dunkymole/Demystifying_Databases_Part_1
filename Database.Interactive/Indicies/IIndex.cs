using System;
using System.Collections.Generic;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal interface IIndex<TKey, TRow>
    {
        IEnumerable<TRow> Scan(Func<TKey, bool> predicate, Retrieval retrieval);
        IEnumerable<TRow> Seek(SeekTarget<TKey> seekTarget, Retrieval retrieval);
    }
}
