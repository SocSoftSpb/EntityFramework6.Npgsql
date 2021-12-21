using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using NUnit.Framework;

namespace EntityFramework6.Npgsql.Tests
{
    public class VectorParametersTests : EntityFrameworkTestBase
    {
        [Test]
        public void CanQueryPostWithBetween()
        {
            using var context = new BloggingContext(ConnectionString);
            
            context.Database.Log = Console.Out.WriteLine;
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Post> posts = objectContext.CreateObjectSet<Post>();

            var query = posts.Where(e => e.BlogId.Between(5, 20)).Select(e => e.Title);
            var queryTrace = ((ObjectQuery)query).ToTraceString();
            Assert.NotNull(queryTrace);

            var lst = query.ToList();
            Assert.NotNull(lst);
        }
        
        [Test]
        public void CanQueryPostByVectorContains()
        {
            using var context = new BloggingContext(ConnectionString);
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;

            var posts = objectContext.CreateObjectSet<Post>();

            context.Database.Log = Console.Out.WriteLine;

            var param = new VectorParameter<int>(new[] { 1, 2, 3, 4 });
            var queryable = posts.Where(e => param.Contains(e.PostId));
            var trace = ((ObjectQuery)queryable).ToTraceString();
            var lst = queryable.ToList();
        }

        [Test]
        public void CanQueryPostWithVariousVectorQuery()
        {
            using var context = new BloggingContext(ConnectionString);
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;

            var posts = objectContext.CreateObjectSet<Post>();

            context.Database.Log = Console.Out.WriteLine;

            var param = new VectorParameter<int>(new[] { 1, 2, 3, 4 });
            
            var queryable = posts.Where(e => param.Where(x => x >= 1 && x <= 3).Contains(e.PostId));
            var trace = ((ObjectQuery)queryable).ToTraceString();
            var lst = queryable.ToList();
                
            // Test plan caching
            // Ensure that in ELinqQueryState.GetExecutionPlan()
            // if (cacheManager.TryCacheLookup(cacheKey, out executionPlan)) -> true
            var queryable2 = posts.Where(e => param.Where(x => x >= 1 && x <= 3).Contains(e.PostId));
            var lst2 = queryable2.ToList();
                
            // Test join
            var qJoin = from post in posts
                join p in param on post.BlogId equals p
                select new { post.PostId, post.Title, AuthorName = post.Blog.Name };
            var lstJoin = qJoin.ToList();
                
            var paramNames = new VectorParameter<string>(new[]{"aaa", "xxx", "bbb"});
                
            // Test join
            var qJoin2 = from post in posts
                join p in param on post.BlogId equals p
                where paramNames.Any(x => post.Title.Contains(x))
                select new { post.PostId, post.Title, AuthorName = post.Blog.Name };
            var lstJoin2 = qJoin2.ToList();

            var qNotAny = posts.Where(e => !param.Contains(e.PostId));
            var trNotAny = ((ObjectQuery)qNotAny).ToTraceString();
            var lstNotAny = qNotAny.ToList();
        }

        [Test]
        public void VectorParametersMustBeSingleInstantiated()
        {
            using var context = new BloggingContext(ConnectionString);

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;

            var posts = objectContext.CreateObjectSet<Post>();

            context.Database.Log = Console.Out.WriteLine;


            var param = new VectorParameter<int>(new[] { 1, 2, 3, 4 });
            var paramNames = new VectorParameter<string>(new[] { "aaa", "xxx", "bbb" });

            // Test join
            var qJoin2 = from b in posts
                join p in param on b.BlogId equals p
                where paramNames.Any(x => b.Title.Contains(x))
                      || paramNames.Any(e => b.Blog.Name == e)
                      || param.Contains(b.BlogId)
                select new { b.PostId, b.Title, AuthorName = b.Blog.Name };

            var trace = ((ObjectQuery)qJoin2).ToTraceString();
            var lstJoin2 = qJoin2.ToList();
        }
        
        private sealed class PostProj
        {
            public int Id { get; set; }
            public string Title { get; set; }
        }

        [Test]
        public void VectorParametersCanBeUsedInCompiledQueries()
        {
            using (var context = new BloggingObjectContext(ConnectionString))
            {
                var comp = CompiledQuery.Compile<BloggingObjectContext, int, int, VectorParameter<int>, IEnumerable<PostProj>>(
                    (ctx, x, y, bookIds) => from b in ctx.Posts
                        where b.PostId.Between(x, y) && bookIds.Contains(b.PostId)
                        select new PostProj { Id = b.PostId, Title = b.Title }
                );

                var vp = new VectorParameter<int>(new[] { 1, 2, 3, 4 });
                var lst = comp(context, 2, 3, vp).ToList();
                
                vp = new VectorParameter<int>(new[] { 1, 2, 3, 4, 5, 6 });
                lst = comp(context, 1, 5, vp).ToList();

                var q = (ObjectQuery)comp(context, 1, 5, vp);
                var trace = q.ToTraceString();
            }
        }
        
    }
}
