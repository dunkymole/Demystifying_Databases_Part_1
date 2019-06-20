using System;
using System.Linq.Expressions;

namespace Database.Interactive
{
    public static class Expressions
    {
        public static string GetMemberName<TIn, TOut>(Expression<Func<TIn, TOut>> expression) 
            => GetMemberName(expression.Body);

        private static string GetMemberName(Expression expression)
        {
            switch (expression)
            {
                case MemberExpression memberExpression:
                    return memberExpression.Member.Name;

                case MethodCallExpression callExpression:
                    return callExpression.Method.Name;

                case UnaryExpression unaryExpression:
                    return GetMemberName(unaryExpression);

                default:
                    throw new ArgumentException("Invalid expression.");
            }
        }

        private static string GetMemberName(UnaryExpression unaryExpression)
        {
            if (unaryExpression.Operand is MethodCallExpression methodExpression)
                return methodExpression.Method.Name;

            return ((MemberExpression) unaryExpression.Operand).Member.Name;
        }   
    }
}