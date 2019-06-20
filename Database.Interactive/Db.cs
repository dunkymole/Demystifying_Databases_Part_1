using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Database.Interactive
{
    public class Db
    {
        public Table<TPrimaryKey, TRow> CreateTable<TPrimaryKey, TRow>(Expression<Func<TRow, TPrimaryKey>> primaryKeySelector, IComparer<TPrimaryKey>? comparer = null) 
            => new Table<TPrimaryKey, TRow>(primaryKeySelector, comparer);
    }
}