using System.Collections.Generic;
using System.IO;
using ProtoBuf;

namespace Data.StackOverflow
{
    public static class DataFiles
    {
        private const string DataPath = @"C:\Users\dunky\RiderProjects\Database\Data\";
        
        public static void DumpFile<T>(IEnumerable<T> items)
        {
            var outPutFile = GetPathForTYpe<T>();
            using (var output = new FileStream(outPutFile, FileMode.Create, FileAccess.Write))
            {
                foreach (var item in items)
                    Serializer.SerializeWithLengthPrefix(output, item, PrefixStyle.Base128);
            }
        }

        public static IEnumerable<T> ReadFile<T>()
        {
            var input = new FileStream(GetPathForTYpe<T>(), FileMode.Open, FileAccess.Read);
            return EnumerableEx.Using(input, Serializer.DeserializeItems<T>(input, PrefixStyle.Base128, 0));
        }
        
        private static string GetPathForTYpe<T>() => Path.Combine(DataPath, $"{typeof(T).Name}s.bin");
    }
}