using System;
using System.Data;
using System.Data.SqlClient;
using System.Reactive.Linq;
using Dapper;

namespace Data.StackOverflow
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Hello World!");

            using (Observable.Interval(TimeSpan.FromSeconds(1)).Subscribe(_ => Console.Write(".")))
            {
                using (var connection = new SqlConnection(@"Server=.\SQLExpress;Database=StackOverflow2010;Integrated Security=true"))
                {
                    DumpTable<Comment>(connection);
                    DumpTable<Post>(connection);
                    DumpTable<PostType>(connection);
                }
            }

            Console.WriteLine("Goodbye Cruel World!");
            Console.ReadKey();
        }

        private static void DumpTable<TProto>(IDbConnection connection) 
            => DataFiles.DumpFile(connection.Query<TProto>($"SELECT * FROM {typeof(TProto).Name}s"));
    }
}