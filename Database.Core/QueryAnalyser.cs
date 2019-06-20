using System;
using System.Collections.Generic;
using Database.Core.Indicies;

namespace Database.Core
{
    public static class QueryAnalyser
    {
        public static IEnumerable<TRow> Pick<TKey, TRow>(SeekTarget<TKey> seekTarget, int indexSize, Func<IEnumerable<TRow>> seek, Func<IEnumerable<TRow>> scan)
        {
            //15% lifted from the literature on b+ trees, its almost certainly not right for us but this is illustrative
            return seekTarget.Keys!.Length / (double) indexSize <= 0.15
                ? seek()
                : scan();
        }
    }
}