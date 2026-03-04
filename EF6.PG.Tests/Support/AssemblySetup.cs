using System.Data.Entity;
using NUnit.Framework;
using EntityFramework6.Npgsql.Tests.Support;
using Microsoft.Extensions.Logging;
using Npgsql;

// ReSharper disable CheckNamespace

[SetUpFixture]
public class AssemblySetup
{
    [OneTimeSetUp]
    public void RegisterDbProvider()
    {

        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole();
        });

        NpgsqlLoggingConfiguration.InitializeLogging(loggerFactory, parameterLoggingEnabled: true);


        DbConfiguration.SetConfiguration(new TestDbConfiguration());
    }
}
