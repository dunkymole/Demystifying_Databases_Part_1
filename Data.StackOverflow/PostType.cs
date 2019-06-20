namespace Data.StackOverflow
{
    public class PostType
    {
        public PostType(int id, string type)
        {
            Id = id;
            Type = type;
        }

        public int Id {get;}
        public string Type {get;}
    }
}