namespace Database.Core.Indicies
{
    public class Retrieval
    {
        public static readonly Retrieval Default = new Retrieval();
        public int? Skip { get; }
        public int? Take { get; }
        public bool Reverse { get; }

        public Retrieval(int? skip = null,
            int? take = null,
            bool reverse = false)
        {
            Skip = skip;
            Take = take;
            Reverse = reverse;
        }
    }
}