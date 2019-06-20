using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using Data.StackOverflow;

namespace Database.Interactive.Demo
{
    class Program
    {
        static void Main()
        {
            var db = new Db();
            var postTypes = db.CreateTable<int, PostType>(p => p.Id);
            postTypes.AddUniqueConstraint(t => t.Type);
            
            var posts = db.CreateTable<int, Post>(p => p.Id);
            posts.AddForeignKeyConstraint(postTypes, s => s.PostTypeId);
            posts.CreateFullTextIndex(c => c.Tags!);
            posts.CreateIndex(p => p.ParentId);
            
            var comments = db.CreateTable<int, Comment>(p => p.Id);
            comments.CreateIndex(c => c.PostId);

            Task.WaitAll(BulkLoadTable(postTypes), BulkLoadTable(posts), BulkLoadTable(comments));
            
            PrintBreak();
            
            Console.WriteLine("Loaded and indexes are up-to-date");

            PrintBreak();
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            RunExample("Which questions created the longest threads?", true, () =>
            {
                return from top10 in posts.GroupByWithCount(c => c.ParentId, id => id != 0).OrderByDescending(d => d.Sum).Take(10)
                       from title in posts.QueryWithIndexSeek(p => p.Id, p => p.Title, new[]{top10.Key})
                       select (title, top10.Sum);
            });
           
            

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            RunExample("What were the most commented posts?", true, () =>
            {
                var top10Commented = comments.GroupByWithCount(c => c.PostId)
                    .OrderByDescending(d=>d.Sum)
                    .Take(10);

                return posts.QueryWithIndexSeek(p => p.Id, p=> $"{p.Body.Substring(0, 50)}.....", top10Commented.Select(s => s.Key));    
            });
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            RunExample("Joining posts to post-type", false, ()=> 
                posts.InnerJoin(postTypes, 
                post => post.PostTypeId, 
                pt => pt.Id,
                (p, pt) => (p.Title, pt.Type)));
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            

            
            
            RunExample("Full text search over 'Tags' for C#", false, ()=> 
                posts.QueryWithFullTextIndex(p => p.Tags!, "C#"));
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            RunExample("Full text search over 'Tags' for PHP", false, () => 
                posts.QueryWithFullTextIndex(p => p.Tags!, "PHP"));
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            var FAKE_POST_TYPE = 999;
            RunExample("Violate the foreign key constraint on 'posts' (to 'post-type')", false, ()=>
            {
                var newPost = new Post(
                    Int32.MaxValue, 
                    null, 
                    null, 
                    "this is a test", 
                    null,
                    null, 
                    null, 
                    DateTime.Now, 
                    null, 
                    DateTime.Now, 
                    null, 
                    "dunkymole", 
                    null, 
                    null, 
                    null, 
                    FAKE_POST_TYPE, //  <---- 
                    0, 
                    "fail", 
                    "violation in progress", 
                    0);

                try
                {
                    posts.Insert(newPost);
                }
                catch (Exception e)
                {
                    using var _ = PrintRed();
                    Console.WriteLine($"Caught foreign constraint violation: {e.Message}");
                }

                return Array.Empty<object>();
            });
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            RunExample("Violate the unique constraint on 'post-type'", false, () =>
            {
                var duplicatePostType = new PostType(FAKE_POST_TYPE, "Question"); //obviously Question already exists :-)
                try
                {
                    postTypes.Insert(duplicatePostType);
                }
                catch (Exception e)
                {
                    using var _ = PrintRed();
                    Console.WriteLine($"Caught unique constraint violation: {e.Message}");
                }    
                
                return Array.Empty<object>();
            });
            
            
            
            
            
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            
            
            
            
            Console.ReadLine();
        }

        
        
        
        
        
        
        private static void RunExample<TResults>(string description, bool printResults, Func<IEnumerable<TResults>> demo)
        {
            // ReSharper disable PossibleMultipleEnumeration
            Console.WriteLine("Hit enter...");
            Console.ReadLine();
            Console.WriteLine(description);
            Console.WriteLine("Hit enter to run...");
            Console.ReadLine();
            var sw = Stopwatch.StartNew();
            var results = demo();
            if (printResults)
            {
                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
            Console.WriteLine();
            Console.WriteLine($"{results.Count():N0}. Ran in: {sw.Elapsed}");
            PrintBreak();
            // ReSharper restore PossibleMultipleEnumeration
        }
        

        private static Task BulkLoadTable<TPKey, TPoco>(Table<TPKey, TPoco> table)
        {
            return Task.Run(() =>
            {
                Console.WriteLine($"Bulk loading '{typeof(TPoco).Name}'");

                table.CheckConstraints = false;
                table.BulkInsert(DataFiles.ReadFile<TPoco>());
                table.CheckConstraints = true;

                Console.WriteLine($"Loaded {table.RowCount:N0} '{typeof(TPoco).Name}'s");
            });
        }

        private static void PrintBreak()
        {
            Console.WriteLine();
            Console.WriteLine("*************************************************************************");
            Console.WriteLine();
            GC.Collect();
        }

        private static IDisposable PrintRed()
        {
            var original = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            return Disposable.Create(original, o=>Console.ForegroundColor = o);
        }
    }
}

