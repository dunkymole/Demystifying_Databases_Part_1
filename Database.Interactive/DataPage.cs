using System;

namespace Database.Interactive
{
    public class DataPage<TPrimaryKey, TRow>
    {
        public static DataPage<TPrimaryKey,TRow> SurrogateForKey(TPrimaryKey primaryKey) 
            => new DataPage<TPrimaryKey, TRow>(primaryKey, default!);
        
        public TRow Row { get; private set; }
        public TPrimaryKey PrimaryKey { get; }

        public DataPage(TPrimaryKey primaryKey, TRow row)
        {
            Row = row;
            PrimaryKey = primaryKey;
        }

        public void Update(TRow row)
        {
            var previous = Row;
            
            if (ReferenceEquals(row, previous))
                throw new Exception("Detected attempt to perform an update reusing the same object");

            Row = row;
        }
    }
}