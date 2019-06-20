using System;
using System.Collections.Generic;

namespace Database.Interactive
{
    public static class FuncEx
    {
        public static Func<TIn, TOut> Memoise<TIn, TOut>(this Func<TIn, TOut> source, int itemsLimit)
        {
            IDictionary<TIn, TOut> cache = new Dictionary<TIn, TOut>();
            return input =>
            {
                if (cache.TryGetValue(input, out var r))
                    return r;
                if(cache.Count < itemsLimit)
                    return cache[input] = source(input);
                return source(input);
            };
        }
    }
}