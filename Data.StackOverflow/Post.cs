using System;

namespace Data.StackOverflow
{
    public class Post
    {
        /*
SELECT 'public ' + DATA_TYPE + IIF(IS_NULLABLE = 'YES', '?', '') + ' ' + COLUMN_NAME + ' {get;}' 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Posts' AND TABLE_SCHEMA='dbo'
         */
        public Post(int id, 
                int? acceptedAnswerId, 
                int? answerCount, 
                string body, 
                DateTime? closedDate, 
                int? commentCount, 
                DateTime? communityOwnedDate, 
                DateTime creationDate, 
                int? favoriteCount, 
                DateTime lastActivityDate, 
                DateTime? lastEditDate, 
                string lastEditorDisplayName, 
                int? lastEditorUserId, 
                int? ownerUserId, 
                int? parentId, 
                int postTypeId, 
                int score, 
                string tags, 
                string title, 
                int viewCount)
        {
            Id = id;
            AcceptedAnswerId = acceptedAnswerId;
            AnswerCount = answerCount;
            Body = body;
            ClosedDate = closedDate;
            CommentCount = commentCount;
            CommunityOwnedDate = communityOwnedDate;
            CreationDate = creationDate;
            FavoriteCount = favoriteCount;
            LastActivityDate = lastActivityDate;
            LastEditDate = lastEditDate;
            LastEditorDisplayName = lastEditorDisplayName;
            LastEditorUserId = lastEditorUserId;
            OwnerUserId = ownerUserId;
            ParentId = parentId;
            PostTypeId = postTypeId;
            Score = score;
            Tags = tags;
            Title = title;
            ViewCount = viewCount;
        }


        public int Id {get;}
        public int? AcceptedAnswerId {get;}
        public int? AnswerCount {get;}
        public string Body {get;}
        public DateTime? ClosedDate {get;}
        public int? CommentCount {get;}
        public DateTime? CommunityOwnedDate {get;}
        public DateTime CreationDate {get;}
        public int? FavoriteCount {get;}
        public DateTime LastActivityDate {get;}
        public DateTime? LastEditDate {get;}
        public string LastEditorDisplayName {get;}
        public int? LastEditorUserId {get;}
        public int? OwnerUserId {get;}
        public int? ParentId {get;}
        public int PostTypeId {get;}
        public int Score {get;}
        public string? Tags {get;}
        public string? Title {get;}
        public int ViewCount {get;}
    }
}