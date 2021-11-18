using System;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using NUnit.Framework;

namespace EntityFramework6.Npgsql.Tests
{
    public class BatchDmlTests : EntityFrameworkTestBase
    {
        [Test]
        public void Test_delete_batch()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            context.Database.Log = Console.Out.WriteLine;

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Post> posts = objectContext.CreateObjectSet<Post>();
            var toDelete = posts.Where(e => e.Title.Contains("aaa") 
                                            && e.Blog.Name.StartsWith("A")
                                            && e.Blog2.Name.StartsWith("B")
                );
                
            var queryTrace = ((ObjectQuery)toDelete).ToTraceString();
            Assert.NotNull(queryTrace);
                
            var toDeleteObjectQuery = (ObjectQuery<Post>)toDelete;
            var deletableQuery = BatchDmlFactory.CreateBatchDeleteQuery(toDeleteObjectQuery, true, 50);
            var deleteTrace = deletableQuery.ToTraceString();
            Assert.NotNull(deleteTrace);

            deletableQuery.Execute();
                    
            tr.Rollback();
        }
        
        [Test]
        public void Test_delete_batch_join()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            context.Database.Log = Console.Out.WriteLine;

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Post> posts = objectContext.CreateObjectSet<Post>();
            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();
            
            var toDelete = posts.Join(blogs, p => p.BlogId, b => b.BlogId, 
                    (p, b) => new BatchDmlFactory.JoinTuple<Post, Blog>{Entity = p, Source = b})
                .Where(
                    e => e.Entity.Title.Contains("aaa")
                         && e.Source.Name.StartsWith("A")
                );
                
            var fromQuery = (ObjectQuery<BatchDmlFactory.JoinTuple<Post, Blog>>)toDelete;
            var deleteQuery = BatchDmlFactory.CreateBatchDeleteJoinQuery<BatchDmlFactory.JoinTuple<Post, Blog>, Post>(fromQuery, false);
            
            // var bb = from b1 in blogs
            //     join b2 in blogs on b1.Name equals b2.Name
            //     select new BlogJoin { BlogId = b1.BlogId, Name = b2.Name };
            //
            // var toDelete = posts.Join(bb, p => p.BlogId, b => b.BlogId, 
            //         (p, b) => new BatchDmlFactory.JoinTuple<Post, BlogJoin>{Entity = p, Source = b})
            //     .Where(
            //         e => e.Entity.Title.Contains("aaa")
            //              && e.Source.Name.StartsWith("A")
            //     );
            //     
            // var fromQuery = (ObjectQuery<BatchDmlFactory.JoinTuple<Post, BlogJoin>>)toDelete;
            // var deleteQuery = BatchDmlFactory.CreateBatchDeleteJoinQuery<BatchDmlFactory.JoinTuple<Post, BlogJoin>, Post>(fromQuery, false);
            
            
            var deleteTrace = deleteQuery.ToTraceString();
            Assert.NotNull(deleteTrace);

            deleteQuery.Execute();
                    
            tr.Rollback();
        }
        
        [Test]
        public void Test_update_batch()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            context.Database.Log = Console.Out.WriteLine;

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Post> posts = objectContext.CreateObjectSet<Post>();
                
            var toUpdate = posts.Where(
                e => e.Title.Contains("aaa")
                     && e.Blog.Name.StartsWith("A")
            );
                
            var fromQuery = (ObjectQuery<Post>)toUpdate;
            var updateQuery = BatchDmlFactory.CreateBatchUpdateQuery(fromQuery,
                e => new Post
                {
                    Title = e.Title + " xxx" + e.Blog.Name,
                    Content = null,
                    BlogId = 100
                }, true);
            var strToUpdate = updateQuery.ToTraceString();
            var result = updateQuery.Execute();
                
            Assert.NotNull(strToUpdate);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }

        class BlogJoin
        {
            public int BlogId { get; set; }
            public string Name { get; set; }
        }

        [Test]
        public void Test_update_batch_join()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            context.Database.Log = Console.Out.WriteLine;

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Post> posts = objectContext.CreateObjectSet<Post>();
            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();

            var bb = from b1 in blogs
                join b2 in blogs on b1.Name equals b2.Name
                select new BlogJoin { BlogId = b1.BlogId, Name = b2.Name };

            var toUpdate = posts.Join(bb, p => p.BlogId, b => b.BlogId, 
                    (p, b) => new BatchDmlFactory.JoinTuple<Post, BlogJoin>{Entity = p, Source = b})
                .Where(
                    e => e.Entity.Title.Contains("aaa")
                         && e.Source.Name.StartsWith("A")
                );
                
            var fromQuery = (ObjectQuery<BatchDmlFactory.JoinTuple<Post, BlogJoin>>)toUpdate;
            var updateQuery = BatchDmlFactory.CreateBatchUpdateJoinQuery(fromQuery,
                e => new Post
                {
                    Title = e.Entity.Title + " xxx" + e.Source.Name,
                    Content = null,
                    BlogId = 100
                }, false);
                
            var strToUpdate = updateQuery.ToTraceString();
            var result = updateQuery.Execute();

            Assert.NotNull(strToUpdate);
            Assert.That(result >= 0);
                
            tr.Rollback();
        }

        [Test]
        public void Test_insert_batch()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            context.Database.Log = Console.Out.WriteLine;

            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();
                
            var toInsert = blogs.Where(
                    e => !e.Name.Contains("aaa")
                )
                .Select(e => new Post
                {
                    PostId = 200 + e.BlogId,
                    Title = "Post in blog " + e.Name,
                    CreationDate = DateTime.Now,
                    BlogId = e.BlogId,
                    Rating = 0,
                });
                
            var fromQuery = (ObjectQuery<Post>)toInsert;
            var insertQuery = BatchDmlFactory.CreateBatchInsertQuery(fromQuery, true);
            var sqlText = insertQuery.ToTraceString();
            var result = insertQuery.Execute();
                
            Assert.NotNull(sqlText);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }
    }
}
