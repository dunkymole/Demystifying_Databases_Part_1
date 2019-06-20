using System;

namespace Database.Interactive
{
    internal class IndexManagerEntry<TPrimaryKey, TRow>
    {
        public object UnderlyingIndex { get; }
        
        private readonly Action<DataPage<TPrimaryKey, TRow>> _onInsert;
        private readonly Action<DataPage<TPrimaryKey, TRow>> _onDelete;
        private readonly Action<(DataPage<TPrimaryKey,TRow> NewValue, TRow PriorValue)> _onUpdate;
        private readonly Action _onClear;

        public static IndexManagerEntry<TPrimaryKey, TRow> FromIndex<TIndexKey>(IIndexManagerItem<TIndexKey, TPrimaryKey, TRow> indexManagerItem)
        {
            return new IndexManagerEntry<TPrimaryKey, TRow>(
                indexManagerItem,
                page =>
                {
                    var addKey = indexManagerItem.CalculateKey(page.Row);
                    indexManagerItem.Add(addKey, page);
                },
                page =>
                {
                    var deleteKey = indexManagerItem.CalculateKey(page.Row);
                    indexManagerItem.Remove(deleteKey, page.PrimaryKey);
                },
                change =>
                {
                    var previousKey = indexManagerItem.CalculateKey(change.PriorValue);
                    var newKey = indexManagerItem.CalculateKey(change.NewValue.Row);
                        
                    //check if the update moved the page into a new bucket
                    if (indexManagerItem.KeyComparer.Compare(previousKey, newKey) != 0)
                    {
                        indexManagerItem.Remove(previousKey, change.NewValue.PrimaryKey);
                        indexManagerItem.Add(newKey, change.NewValue);
                    }
                },
                onClear: indexManagerItem.Clear);
        }
        
        private IndexManagerEntry(object underlyingIndex,
            Action<DataPage<TPrimaryKey, TRow>> onInsert, 
            Action<DataPage<TPrimaryKey, TRow>> onDelete, 
            Action<(DataPage<TPrimaryKey,TRow> NewValue, TRow PriorValue)> onUpdate,
            Action onClear)
        {
            UnderlyingIndex = underlyingIndex;
            _onInsert = onInsert;
            _onDelete = onDelete;
            _onUpdate = onUpdate;
            _onClear = onClear;
        }

        public void PropagateInsert(DataPage<TPrimaryKey, TRow> page) => _onInsert(page);

        public void PropagateDelete(DataPage<TPrimaryKey, TRow> page) => _onDelete(page);

        public void PropagateUpdate((DataPage<TPrimaryKey, TRow> NewValue, TRow PriorValue) page) => _onUpdate(page);

        public void Clear() => _onClear();
    }
}