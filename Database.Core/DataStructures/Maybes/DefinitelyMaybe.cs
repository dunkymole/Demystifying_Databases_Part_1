namespace Database.Core.DataStructures.Maybes
{
    public class DefinitelyMaybe<T> : IMaybe<T>
    {
        private readonly T _value;
        public DefinitelyMaybe(T value) => _value = value;
        public bool HasValue => true;
        public T Value => _value;

        public override string ToString()
        {
            return $"{nameof(HasValue)}: {true}, {nameof(Value)}: {Value}";
        }
    }
}