﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Sql;
using System.Data.Entity.Infrastructure.DependencyResolution;
using Npgsql.SqlGenerators;
using DbConnection = System.Data.Common.DbConnection;
using DbCommand = System.Data.Common.DbCommand;
using System.Data.Common;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Objects;
using JetBrains.Annotations;
using NpgsqlTypes;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql
{
    public class NpgsqlServices : DbProviderServices
    {
        public static NpgsqlServices Instance { get; } = new NpgsqlServices();

        public NpgsqlServices()
        {
            AddDependencyResolver(new SingletonDependencyResolver<Func<MigrationSqlGenerator>>(
                () => new NpgsqlMigrationSqlGenerator(), nameof(Npgsql)));
        }

        protected override DbCommandDefinition CreateDbCommandDefinition([NotNull] DbProviderManifest providerManifest, [NotNull] DbCommandTree commandTree)
            => CreateCommandDefinition(CreateDbCommand(((NpgsqlProviderManifest)providerManifest).Version, commandTree));

        internal DbCommand CreateDbCommand(Version serverVersion, DbCommandTree commandTree)
        {
            if (commandTree == null)
                throw new ArgumentNullException(nameof(commandTree));

            var command = new NpgsqlCommand();

            foreach (var parameter in commandTree.Parameters)
            {
                var dbParameter = CreateNpgsqlParameter(parameter.Key, parameter.Value);
                command.Parameters.Add(dbParameter);
            }

            TranslateCommandTree(serverVersion, commandTree, command);

            return command;
        }

        static NpgsqlParameter CreateNpgsqlParameter(string parameterName, TypeUsage typeUsage)
        {
            NpgsqlDbType npgsqlDbType;

            if (typeUsage.EdmType.BuiltInTypeKind == BuiltInTypeKind.VectorParameterType)
            {
                // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
                var primitiveTypeKind = ((VectorParameterType)typeUsage.EdmType).ElementType.PrimitiveTypeKind;
                var elType = NpgsqlProviderManifest.GetNpgsqlDbType(primitiveTypeKind);
                if (elType == NpgsqlDbType.Unknown && primitiveTypeKind == PrimitiveTypeKind.String)
                    elType = NpgsqlDbType.Text;
                npgsqlDbType = NpgsqlDbType.Array | elType;
            }
            else
            {
                npgsqlDbType = NpgsqlProviderManifest.GetNpgsqlDbType(((PrimitiveType)typeUsage.EdmType).PrimitiveTypeKind);
            }

            var dbParameter = new NpgsqlParameter
            {
                ParameterName = parameterName,
                NpgsqlDbType = npgsqlDbType
            };
            return dbParameter;
        }
        
        private static void SetVectorParameterProperties(NpgsqlParameter npgParameter, VectorParameterTypeMapping mapping)
        {
            var primitiveTypeKind = mapping.VectorParameterType.ElementType.PrimitiveTypeKind;
            var elType = NpgsqlProviderManifest.GetNpgsqlDbType(primitiveTypeKind);
            if (elType == NpgsqlDbType.Unknown && primitiveTypeKind == PrimitiveTypeKind.String)
                elType = NpgsqlDbType.Text;

            npgParameter.NpgsqlDbType = NpgsqlDbType.Array | elType;
        }

        public override void SetParameterValue(MetadataWorkspace metadataWorkspace, DbParameter parameter, object value)
        {
            if (value is VectorParameter vectorParameter)
            {
                if (!(parameter is NpgsqlParameter npgParameter))
                    throw new InvalidOperationException("NpgsqlParameter expected.");
            
                var mapping = GetVectorParameterTypeMapping(metadataWorkspace, vectorParameter);
                SetVectorParameterProperties(npgParameter, mapping);
                npgParameter.Value = value;
            }
            else
            {
                base.SetParameterValue(metadataWorkspace, parameter, value);
            }
        }

        protected override void SetDbParameterValue(MetadataWorkspace metadataWorkspace, DbParameter parameter, TypeUsage parameterType, object value)
        {
            base.SetDbParameterValue(metadataWorkspace, parameter, parameterType, value);
            ConvertValueToNumericIfEnum(parameter);
        }

        // Npgsql > 4.0 does strict type checks on integral values and fails with enums passed with numeric DbType.
        static void ConvertValueToNumericIfEnum(DbParameter parameter)
        {
            var parameterValueObjectType = parameter.Value.GetType();

            if (!parameterValueObjectType.IsEnum)
            {
                return;
            }

            var underlyingType = Enum.GetUnderlyingType(parameterValueObjectType);
            parameter.Value = Convert.ChangeType(parameter.Value, underlyingType);
        }

        internal void TranslateCommandTree(Version serverVersion, DbCommandTree commandTree, DbCommand command, bool createParametersForNonSelect = true)
        {
            SqlBaseGenerator sqlGenerator;

            var metadataWorkspace = commandTree.MetadataWorkspace;

            DbQueryCommandTree select;
            DbInsertCommandTree insert;
            DbUpdateCommandTree update;
            DbDeleteCommandTree delete;
            if ((select = commandTree as DbQueryCommandTree) != null)
                sqlGenerator = new SqlSelectGenerator(metadataWorkspace, select);
            else if ((insert = commandTree as DbInsertCommandTree) != null)
                sqlGenerator = new SqlInsertGenerator(metadataWorkspace, insert);
            else if ((update = commandTree as DbUpdateCommandTree) != null)
                sqlGenerator = new SqlUpdateGenerator(metadataWorkspace, update);
            else if ((delete = commandTree as DbDeleteCommandTree) != null)
                sqlGenerator = new SqlDeleteGenerator(metadataWorkspace, delete);
            else
            {
                // TODO: get a message (unsupported DbCommandTree type)
                throw new ArgumentException();
            }
            sqlGenerator.CreateParametersForConstants = select == null && createParametersForNonSelect;
            sqlGenerator.Command = (NpgsqlCommand)command;
            sqlGenerator.Version = serverVersion;

            sqlGenerator.BuildCommand(command);
        }

        protected override string GetDbProviderManifestToken([NotNull] DbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            var serverVersion = "";
            UsingPostgresDbConnection((NpgsqlConnection)connection, conn => {
                serverVersion = conn.ServerVersion;
            });
            return serverVersion;
        }

        protected override DbProviderManifest GetDbProviderManifest([NotNull] string versionHint)
        {
            if (versionHint == null)
                throw new ArgumentNullException(nameof(versionHint));
            return new NpgsqlProviderManifest(versionHint);
        }

        protected override bool DbDatabaseExists([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            var exists = false;
            UsingPostgresDbConnection((NpgsqlConnection)connection, conn =>
            {
                using (var command = new NpgsqlCommand("select count(*) from pg_catalog.pg_database where datname = '" + connection.Database + "';", conn))
                    exists = Convert.ToInt32(command.ExecuteScalar()) > 0;
            });
            return exists;
        }

        protected override void DbCreateDatabase([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            UsingPostgresDbConnection((NpgsqlConnection)connection, conn =>
            {
                var sb = new StringBuilder();
                sb.Append("CREATE DATABASE \"");
                sb.Append(connection.Database);
                sb.Append("\"");
                if (conn.Settings.EntityTemplateDatabase != null)
                {
                    sb.Append(" TEMPLATE \"");
                    sb.Append(conn.Settings.EntityTemplateDatabase);
                    sb.Append("\"");
                }

                using (var  command = new NpgsqlCommand(sb.ToString(), conn))
                    command.ExecuteNonQuery();
            });
        }

        protected override void DbDeleteDatabase([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            UsingPostgresDbConnection((NpgsqlConnection)connection, conn =>
            {
                //Close all connections in pool or exception "database used by another user appears"
                NpgsqlConnection.ClearAllPools();
                KillDatabaseSessions(conn, connection.Database);
                using (var command = new NpgsqlCommand("DROP DATABASE \"" + connection.Database + "\";", conn))
                    command.ExecuteNonQuery();
            });

            void KillDatabaseSessions(NpgsqlConnection conn, string databaseName)
            {
                try
                {
                    using (var command = new NpgsqlCommand("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = @p0;", conn))
                    {
                        command.Parameters.Add(new NpgsqlParameter<string>("@p0", databaseName));
                        command.ExecuteNonQuery();
                    }
                }
                catch
                {
                    // do nothinng
                }
            }
        }

        static void UsingPostgresDbConnection(NpgsqlConnection connection, Action<NpgsqlConnection> action)
        {
            var connectionBuilder = new NpgsqlConnectionStringBuilder(connection.ConnectionString)
            {
                Database = connection.Settings.EntityAdminDatabase ?? "template1",
                Pooling = false
            };

            using (var masterConnection = connection.CloneWith(connectionBuilder.ConnectionString))
            {
                masterConnection.Open();//using's Dispose will close it even if exception...
                action(masterConnection);
            }
        }
    }
}
