using System.Collections.Generic;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    internal interface IInvertedIndex<out TRow>
    {
        IEnumerable<TRow> Seek(string searchTerm, Retrieval retrieval);
    }
}