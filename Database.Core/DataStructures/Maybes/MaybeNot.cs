using System;

namespace Database.Core.DataStructures.Maybes
{
    public class MaybeNot<T> : IMaybe<T>
    {
        public static IMaybe<T> Instance = new MaybeNot<T>();
        private MaybeNot(){}
        public bool HasValue => false;
        public T Value => throw new ArgumentException("Attempted to read the value of an empty Maybe");

        public override string ToString()
        {
            return $"{nameof(HasValue)}: {false}, {nameof(Value)}: <>";
        }
    }
}