using System;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using JetBrains.Annotations;
using NUnit.Framework;

namespace EntityFramework6.Npgsql.Tests
{
    public class TempTableTests : EntityFrameworkTestBase
    {
        [SuppressMessage("ReSharper", "UnusedMember.Local")]
        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        [UsedImplicitly]
        sealed class DynamicPost
        {
            public int PostId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public byte Rating { get; set; }
            public DateTime CreationDate { get; set; }
            public int BlogId { get; set; }
        }
        
        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        sealed class TempPost
        {
            public int PostId { get; set; }
            public string Title { get; set; }
            public int BlogId { get; set; }
        }
        
        [Test]
        public void Test_Dynamic_Sql()
        {
            using var context = new BloggingContext(ConnectionString);
            
            var definingQuery = "SELECT" + " \"PostId\",  \"Title\", \"Content\", \"Rating\", \"CreationDate\", \"BlogId\" FROM \"dbo\".\"Posts\"";
            var dynQ = context.DynamicQuery<DynamicPost>(definingQuery);
            var lst = context.DynamicQuery<DynamicPost>(definingQuery).ToList();
            Assert.NotNull(lst);
                
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            var blogs = objectContext.CreateObjectSet<Blog>();
            var queryBlogs = from b in blogs
                join d in dynQ on b.BlogId equals d.BlogId
                select new
                {
                    b.Name,
                    d.Rating
                };

            var queryBlogsTrace = ((ObjectQuery)queryBlogs).ToTraceString();
            var lstBlogs = queryBlogs.ToList();
                
            Assert.NotNull(queryBlogsTrace);
            Assert.NotNull(lstBlogs);
        }

        [Test]
        public void CanInsertTempTable() => CanInsertTempTableInternal(false);

        [Test]
        public void CanInsertTempTableWithCache() => CanInsertTempTableInternal(true);

        void CanInsertTempTableInternal(bool withCache)
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);

            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();
            var dynQ = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post");
            var strDynQ = ((ObjectQuery)dynQ).ToTraceString();
                    
            Assert.NotNull(strDynQ);
                
            var toInsert = blogs.Where(
                    e => !e.Name.Contains("aaa")
                )
                .Take(20)
                .Select(
                    e => new TempPost
                    {
                        PostId = e.BlogId + 100500,
                        Title = "Book written by " + e.Name,
                        BlogId = e.BlogId
                    });
                
            var fromQuery = (ObjectQuery<TempPost>)toInsert;
            var options = context.CreateDynamicQueryOptions(typeof(TempPost));
            if (withCache)
                options.UniqueSetName = "UUQ_t_POST";
            var insertQuery = BatchDmlFactory.CreateBatchInsertDynamicTableQuery(fromQuery, "pg_temp.#t_Post", options, true);
            var strInsert = insertQuery.ToTraceString();
            var result = insertQuery.Execute();
            
            Assert.NotNull(strInsert);
            Assert.That(result >= 0);

            tr.Rollback();
        }
        
        [Test]
        public void CanInsertTempTableWithParameters() => CanInsertTempTableInternalWithParameters(false);
        
        [Test]
        public void CanInsertTempTableWithParametersAndCache() => CanInsertTempTableInternalWithParameters(true);
        
        void CanInsertTempTableInternalWithParameters(bool withCache)
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);

            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();
            var dynQ = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post");
            var strDynQ = ((ObjectQuery)dynQ).ToTraceString();
                    
            Assert.NotNull(strDynQ);

            var toAdd = 100500;
            var str = "Blog";
                
            var toInsert = blogs.Where(
                    e => !e.Name.Contains("aaa")
                )
                .Take(20)
                .Select(
                    e => new TempPost
                    {
                        PostId = e.BlogId + toAdd,
                        Title = str + " written by " + e.Name,
                        BlogId = e.BlogId
                    });
                
            var fromQuery = (ObjectQuery<TempPost>)toInsert;
            var options = context.CreateDynamicQueryOptions(typeof(TempPost));
            if (withCache)
                options.UniqueSetName = "UUQ_t_POST";
            var insertQuery = BatchDmlFactory.CreateBatchInsertDynamicTableQuery(fromQuery, "pg_temp.#t_Post", options, true);
            var strInsert = insertQuery.ToTraceString();
            var result = insertQuery.Execute();
                    
            Assert.NotNull(strInsert);
            Assert.That(result >= 0);

            tr.Rollback();
        }
        
        [Test]
        public void CanUpdateTempTable()
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            const string sqlInsert = @"INSERT " + @"INTO pg_temp.""#t_Post"" VALUES (1, 'Post 1', 1), (2, 'Post 2', 1), (3, 'Post 3', 2), (4, 'Post 4', 3)";

            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);
            objectContext.ExecuteStoreCommand(sqlInsert);

            var options = BloggingContext.DynamicQueryUtils.CreateDynamicQueryOptions(typeof(TempPost));
            var tempBooks = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post", options);
                    
            var toUpdate = tempBooks.Where(
                e => !e.Title.Contains("aaa") && e.BlogId > 1
            );
                    
            var fromQuery = (ObjectQuery<TempPost>)toUpdate;
                    
            var updateQuery = BatchDmlFactory.CreateBatchUpdateDynamicTableQuery(
                fromQuery,
                options,
                e => new TempPost()
                {
                    Title = e.Title + " aaa",
                }, true);
            var strToUpdate = updateQuery.ToTraceString();
            var result = updateQuery.Execute();
            
            Assert.NotNull(strToUpdate);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }
        
        [Test]
        public void CanUpdateTempTableWithJoin()
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            const string sqlInsert = @"INSERT " + @"INTO pg_temp.""#t_Post"" VALUES (1, 'Post 1', 1), (2, 'Post 2', 1), (3, 'Post 3', 2), (4, 'Post 4', 3)";

            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);
            objectContext.ExecuteStoreCommand(sqlInsert);

            IQueryable<Blog> blogs = objectContext.CreateObjectSet<Blog>();
            var options = BloggingContext.DynamicQueryUtils.CreateDynamicQueryOptions(typeof(TempPost));
            var tempBooks = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post", options);
                    
            var toUpdate = tempBooks.Join(blogs, b => b.BlogId, a => a.BlogId, (b, a) => new BatchDmlFactory.JoinTuple<TempPost, Blog>{Entity = b, Source = a})
                .Where(
                    e => !e.Entity.Title.Contains("aaa") && e.Source.Name != "Ralph"
                );
                    
            var fromQuery = (ObjectQuery<BatchDmlFactory.JoinTuple<TempPost, Blog>>)toUpdate;
                    
            var updateQuery = BatchDmlFactory.CreateBatchUpdateDynamicTableJoinQuery(
                fromQuery,
                options,
                x => new TempPost()
                {
                    Title = x.Entity.Title + " " + x.Source.Name,
                }, true);
            var strToUpdate = updateQuery.ToTraceString();
            var result = updateQuery.Execute();
                    
            Assert.NotNull(strToUpdate);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }
        [Test]
        public void CanDeleteTempTable()
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            const string sqlInsert = @"INSERT " + @"INTO pg_temp.""#t_Post"" VALUES (1, 'Post 1', 1), (2, 'Post 2', 1), (3, 'Post 3', 2), (4, 'Post 4', 3)";
            
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);
            objectContext.ExecuteStoreCommand(sqlInsert);

            var options = BloggingContext.DynamicQueryUtils.CreateDynamicQueryOptions(typeof(TempPost));
            var tempBooks = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post", options);
                    
            var toDelete = tempBooks.Where(
                e => !e.Title.Contains("aaa") && e.BlogId > 1
            );
                    
            var fromQuery = (ObjectQuery<TempPost>)toDelete;
                    
            var deleteQuery = BatchDmlFactory.CreateBatchDeleteDynamicTableQuery(
                fromQuery, options, true, 120);
                    
            var strToDelete = deleteQuery.ToTraceString();
            var result = deleteQuery.Execute();
                    
            Assert.NotNull(strToDelete);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }
        
        [Test]
        public void CanDeleteTempTableWithJoin()
        {
            const string sqlCreate = "CREATE TEMPORARY " + "TABLE pg_temp.\"#t_Post\"(\"PostId\" INT NOT NULL, \"Title\" TEXT NULL, \"BlogId\" INT NOT NULL);";
            const string sqlInsert = @"INSERT " + @"INTO pg_temp.""#t_Post"" VALUES (1, 'Post 1', 1), (2, 'Post 2', 1), (3, 'Post 3', 2), (4, 'Post 4', 3)";
            
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();
            
            var objectContext = ((IObjectContextAdapter)context).ObjectContext;
            objectContext.Connection.Open();
            objectContext.ExecuteStoreCommand(sqlCreate);
            objectContext.ExecuteStoreCommand(sqlInsert);

            IQueryable<Blog> authors = objectContext.CreateObjectSet<Blog>();
            var options = BloggingContext.DynamicQueryUtils.CreateDynamicQueryOptions(typeof(TempPost));
            var tempBooks = context.DynamicQuery<TempPost>("TABLE:pg_temp.#t_Post", options);
                    
            var toDelete = tempBooks.Join(authors, b => b.BlogId, a => a.BlogId, (b, a) => new BatchDmlFactory.JoinTuple<TempPost, Blog>{Entity = b, Source = a})
                .Where(
                    e => !e.Entity.Title.Contains("aaa") && e.Source.Name != "Ralph"
                );
                    
            var fromQuery = (ObjectQuery<BatchDmlFactory.JoinTuple<TempPost, Blog>>)toDelete;
                    
            var deleteQuery = BatchDmlFactory.CreateBatchDeleteDynamicTableJoinQuery<BatchDmlFactory.JoinTuple<TempPost, Blog>, TempPost>(
                fromQuery, options, true, 120);
                    
            var strToDelete = deleteQuery.ToTraceString();
            var result = deleteQuery.Execute();
                    
            Assert.NotNull(strToDelete);
            Assert.That(result >= 0);
                    
            tr.Rollback();
        }

        [Test]
        public void CanCreateDateTime()
        {
            using var context = new BloggingContext(ConnectionString);
            using var tr = context.Database.BeginTransaction();

            var qq = context.Blogs.Select(e => new { e.BlogId, dt = DbFunctions.CreateDateTime(2020, 01, 01, 0, 0, 0) });
            var ss = qq.ToString();
            
            tr.Rollback();
        }
       
    }
}
