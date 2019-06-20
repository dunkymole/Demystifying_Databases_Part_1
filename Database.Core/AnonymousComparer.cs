using System;
using System.Collections.Generic;

namespace Database.Core
{
    public class AnonymousComparer<TParent, TKey> : IComparer<TParent>
    {
        private readonly Func<TParent, TKey> _keySelector;
        private readonly IComparer<TKey> _keyComparer;

        public AnonymousComparer(Func<TParent, TKey> keySelector, IComparer<TKey>? keyComparer)
        {
            _keySelector = keySelector;
            _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        }

        public IComparer<TKey> KeyComparer => _keyComparer;

        public int Compare(TParent x, TParent y)
        {
            return _keyComparer.Compare(_keySelector(x), _keySelector(y));
        }
    }
}