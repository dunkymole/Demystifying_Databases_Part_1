    namespace Database.Core.DataStructures.Maybes
{
    public interface IMaybe<T>
    {
        bool HasValue { get; }
        T Value { get; }
    }
}