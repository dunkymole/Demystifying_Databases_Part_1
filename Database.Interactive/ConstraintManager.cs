using System;
using System.Collections.Generic;

namespace Database.Interactive
{
    internal class ConstraintManager<TPrimaryKey, TRow>
    {
        private readonly List<Action<TRow>> _upsertConstraints = new List<Action<TRow>>();
        private readonly List<Action<TPrimaryKey>> _deleteConstraints = new List<Action<TPrimaryKey>>();
        public bool Enabled { get; set; }

        public void BeforeDelete(TPrimaryKey key)
        {
            if (!Enabled) return;

            lock (_deleteConstraints)
            {
                foreach (var deleteConstraint in _deleteConstraints) 
                    deleteConstraint(key);    
            }
        }

        public void BeforeInsertUpdate(TRow row)
        {
            if (!Enabled) return;

            lock (_upsertConstraints)
            {
                foreach (var upsertConstraint in _upsertConstraints) 
                    upsertConstraint(row);    
            }
        }

        public void RegisterInsertUpdateConstraint(Action<TRow> beforeUpdateInsert)
        {
            lock (_upsertConstraints) 
                _upsertConstraints.Add(beforeUpdateInsert);
        }

        public void RegisterDeleteConstraint(Action<TPrimaryKey> primaryKey)
        {
            lock (_deleteConstraints) 
                _deleteConstraints.Add(primaryKey);
        }
    }
}