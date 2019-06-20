using System;
using System.Collections.Generic;

namespace Data.StackOverflow
{
    public static class EnumerableEx
    {
        public static IEnumerable<T> Using<T>(IDisposable disposable, IEnumerable<T> source)
        {
            using (disposable)
            {
                foreach (var item in source)
                    yield return item;
            }
        }
    }
}