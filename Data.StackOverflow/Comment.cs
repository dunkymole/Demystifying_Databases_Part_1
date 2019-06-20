using System;

namespace Data.StackOverflow
{
    /*
SELECT 'public ' + DATA_TYPE + IIF(IS_NULLABLE = 'YES', '?', '') + ' ' + COLUMN_NAME + ' {get;}' 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Comments' AND TABLE_SCHEMA='dbo'
     */
    public class Comment
    {
        public Comment(int id, DateTime creationDate, int postId, int? score, string text, int? userId)
        {
            Id = id;
            CreationDate = creationDate;
            PostId = postId;
            Score = score;
            Text = text;
            UserId = userId;
        }

        public int Id {get;}
        public DateTime CreationDate {get;}
        public int PostId {get;}
        public int? Score {get;}
        public string Text {get;}
        public int? UserId {get;}
    }
}