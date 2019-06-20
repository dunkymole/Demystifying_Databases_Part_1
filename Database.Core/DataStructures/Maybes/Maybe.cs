using System;

namespace Database.Core.DataStructures.Maybes
{
    public static class Maybe
    {
        public static IMaybe<TOut> Select<TIn, TOut>(this IMaybe<TIn> source, Func<TIn, TOut> projection)
            => source.HasValue ? Return(projection(source.Value)) : Empty<TOut>();
        public static IMaybe<T> Empty<T>() => MaybeNot<T>.Instance;
        public static IMaybe<T> Return<T>(T value) => new DefinitelyMaybe<T>(value);
    }
}