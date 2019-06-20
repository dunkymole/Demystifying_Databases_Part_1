using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Database.Core.DataStructures.Trees.RedBlack;
using Database.Core.Indicies;

namespace Database.Interactive.Indicies
{
    //a simple, scored inverted index. score is based on word occurrence, not position, recording the position of word occurrences
    //would permit a higher scores for a "run of words" and therefore rank phrase matches higher. there are many ways to tweak the scoring,
    //such as to give words scores equal to the length of the word
    
    internal class InvertedIndex<TPrimaryKey, TRow> : IIndexManagerItem<string, TPrimaryKey, TRow>, IInvertedIndex<TRow> 
    {
        private readonly IComparer<TPrimaryKey> _clusteredIndexComparer;
        private readonly Func<TRow, string> _textFieldSelector;
        private readonly ConcurrentDictionary<string, Lazy<ConcurrentDictionary<TPrimaryKey, (TRow Row, int ObservationCount)>>> _invertedIndex;

        //as an exercise for the reader, implement a "forgiving" string comparer for example one which has a certain tolerance to 
        //Levenshtein distance https://en.wikipedia.org/wiki/Levenshtein_distance
        
        internal InvertedIndex(Expression<Func<TRow, string>> textFieldSelector,
            IEqualityComparer<string> stringComparer, 
            IComparer<TPrimaryKey> clusteredIndexComparer)
        {
            _clusteredIndexComparer = clusteredIndexComparer;
            _textFieldSelector = textFieldSelector.Compile();
            KeyComparer = new StringComparerAdapter(stringComparer);
            _invertedIndex = new ConcurrentDictionary<string, Lazy<ConcurrentDictionary<TPrimaryKey, (TRow Row, int ObservationCount)>>>(stringComparer);
        }

        public void Add(string text, DataPage<TPrimaryKey, TRow> page)
        {
            Parallel.ForEach(NormalisedSplit(text), word =>
            {
                var perWord = _invertedIndex.GetOrAdd(word, _ => new Lazy<ConcurrentDictionary<TPrimaryKey, (TRow Row, int ObservationCount)>>(
                    ()=>new ConcurrentDictionary<TPrimaryKey, (TRow, int)>(), LazyThreadSafetyMode.ExecutionAndPublication));
                
                var preIncrement = perWord.Value.GetOrAdd(page.PrimaryKey, (_,p) => (p.Row, 0), page);
                
                perWord.Value[page.PrimaryKey] = (preIncrement.Row, preIncrement.ObservationCount + 1);
            });
        }

        public void Remove(string text, TPrimaryKey primaryKey)
        {
            Parallel.ForEach(NormalisedSplit(text), word =>
            {
                if (!_invertedIndex.TryGetValue(word, out var wordObservations))
                    throw new Exception($"Failed to find inverted index entry for word: '{word}'");

                var current = wordObservations.Value[primaryKey];
                if (current.ObservationCount != 1)
                    wordObservations.Value[primaryKey] = (current.Row, current.ObservationCount - 1);
                else
                    wordObservations.Value.Remove(primaryKey, out _);

                if (wordObservations.Value.Count == 0)
                    _invertedIndex.Remove(word, out _);
            });
        }

        public string CalculateKey(TRow row) => _textFieldSelector(row);

        public IComparer<string> KeyComparer { get; }

        public void Clear() => _invertedIndex.Clear();

        public IEnumerable<TRow> Seek(string searchTerm, Retrieval retrieval)
        {
            var words = NormalisedSplit(searchTerm);

            //accumulate the score for each row from each word of the search phrase
            var rowScores = new RedBlackTreeMap<TPrimaryKey, (TRow Row, int ObservationCount)>(_clusteredIndexComparer);

            Parallel.ForEach(words, word =>
            {
                if (!_invertedIndex.TryGetValue(word, out var observations)) return;
                foreach (var (key, score) in observations.Value)
                {
                    rowScores.AddOrReplace(key,
                        () => score,
                        accumalatedScore => (accumalatedScore.Row,
                            accumalatedScore.ObservationCount + score.ObservationCount));
                }
            });
            
            var scoredRows = rowScores.GetItems(false).Select(kv => kv.Value);
            
            return (retrieval.Reverse ? scoredRows.OrderBy(s=>s.ObservationCount) : scoredRows.OrderByDescending(s=>s.ObservationCount))
                .Skip(retrieval.Skip ?? 0)
                .Take(retrieval.Take ?? Int32.MaxValue)
                .Select(r=>r.Row)
                .ToArray()
                .AsEnumerable();
        }

        //more advanced text search indexes will use language comprehensions such as word stemming: 
        //https://en.wikipedia.org/wiki/Stemming
        //other techniques such as fanning out with synonyms can introduce semantic search
        
        private readonly string[] Ignore = {",", " ", ".", ";", ">", "<", "the", "be", "to", "of", "and", "a", "in", "that", "have", "i"};
        private IEnumerable<string> NormalisedSplit(string input) 
            => input == null ? Array.Empty<string>() : input.ToLower().Split(Ignore, StringSplitOptions.RemoveEmptyEntries);

        private class StringComparerAdapter : IComparer<string>
        {
            private readonly IEqualityComparer<string> _stringComparer;

            public StringComparerAdapter(IEqualityComparer<string> stringComparer) => _stringComparer = stringComparer;

            public int Compare(string x, string y)
            {
                if (_stringComparer.Equals(x, y)) return 0;
                //good enough for this purpose
                return -1;
            }
        }
    }
    
}