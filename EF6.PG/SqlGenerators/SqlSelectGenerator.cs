using System;
using System.Linq;
using System.Data.Common;
using System.Diagnostics;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Text;

namespace Npgsql.SqlGenerators
{
    internal class SqlSelectGenerator : SqlBaseGenerator
    {
        readonly DbQueryCommandTree _commandTree;

        protected override DbDmlOperation DmlOperation => _commandTree.DmlOperation; 

        public SqlSelectGenerator(DbQueryCommandTree commandTree)
        {
            _commandTree = commandTree;
        }

        protected SqlSelectGenerator()
        {
            // used only for other generators such as returning
        }

        public override VisitedExpression Visit(DbPropertyExpression expression)
        {
            /*
             * Algorithm for finding the correct reference expression: "Collection"."Name"
             * The collection is always a leaf InputExpression, found by lookup in _refToNode.
             * The name for the collection is found using node.TopName.
             *
             * We must now follow the path from the leaf down to the root,
             * and make sure the column is projected all the way down.
             *
             * We need not project columns at a current InputExpression.
             * For example, in
             *  SELECT ? FROM <from> AS "X" WHERE "X"."field" = <value>
             * we use the property "field" but it should not be projected.
             * Current expressions are stored in _currentExpressions.
             * There can be many of these, for example if we are in a WHERE EXISTS (SELECT ...) or in the right hand side of an Apply expression.
             *
             * At join nodes, column names might have to be renamed, if a name collision occurs.
             * For example, the following would be illegal,
             *  SELECT "X"."A" AS "A", "Y"."A" AS "A" FROM (SELECT 1 AS "A") AS "X" CROSS JOIN (SELECT 1 AS "A") AS "Y"
             * so we write
             *  SELECT "X"."A" AS "A", "Y"."A" AS "A_Alias<N>" FROM (SELECT 1 AS "A") AS "X" CROSS JOIN (SELECT 1 AS "A") AS "Y"
             * The new name is then propagated down to the root.
             */

            var name = expression.Property.Name;
            var from = expression.Instance.ExpressionKind == DbExpressionKind.Property
                ? ((DbPropertyExpression)expression.Instance).Property.Name
                : ((DbVariableReferenceExpression)expression.Instance).VariableName;

            string cast = null;
            if (expression.Property.TypeUsage.EdmType.Name == "xml")
            {
                cast = "text";
            }

            var node = RefToNode[from];
            from = node.TopName;
            while (node != null)
            {
                foreach (var item in node.Selects)
                {
                    if (CurrentExpressions.Contains(item.Exp))
                        continue;

                    var use = new StringPair(from, name);

                    if (!item.Exp.ColumnsToProject.ContainsKey(use))
                    {
                        var oldName = name;
                        while (item.Exp.ProjectNewNames.Contains(name))
                            name = oldName + "_" + NextAlias();
                        item.Exp.ColumnsToProject[use] = name;
                        item.Exp.ProjectNewNames.Add(name);
                    }
                    else
                        name = item.Exp.ColumnsToProject[use];
                    from = item.AsName;
                }
                node = node.JoinParent;
            }
            return new ColumnReferenceExpression { Variable = from, Name = name, Cast = cast };
        }

        // must provide a NULL of the correct type
        // this is necessary for certain types of union queries.
        public override VisitedExpression Visit(DbNullExpression expression)
            => new CastExpression(new LiteralExpression("NULL"), GetDbType(expression.ResultType.EdmType));

        public override void BuildCommand(DbCommand command)
        {
            var dmlOperation = _commandTree.DmlOperation;
            
            Debug.Assert(command is NpgsqlCommand);
            Debug.Assert(_commandTree.Query is DbProjectExpression);

            if (dmlOperation is DbDmlInsertOperation { FromObjectQuery: { } } insertOperation)
            {
                BuildInsertFromObjectQuery(command, insertOperation);
                return;
            }
            
            var ve = _commandTree.Query.Accept(this);
            Debug.Assert(ve is InputExpression);
            var pe = (InputExpression)ve;

            VisitedExpression result = pe;

            var finalProjection = pe.Projection;
            
            if (dmlOperation != null)
            {
                switch (dmlOperation.Kind)
                {
                case DbDmlOperationKind.Delete:
                {
                    var delOp = (DbDmlDeleteOperation)dmlOperation;

                    var dmlExpr = new DmlDeleteExpression(pe, delOp);
                    finalProjection = dmlExpr.GetProjection();
                    result = dmlExpr;
                    break;
                }
                case DbDmlOperationKind.Update:
                {
                    var updOp = (DbDmlUpdateOperation)dmlOperation;

                    var dmlExpr = new DmlUpdateExpression(pe, updOp);
                    finalProjection = dmlExpr.GetProjection();
                    result = dmlExpr;
                    break;
                }
                case DbDmlOperationKind.Insert:
                {
                    var insOp = (DbDmlInsertOperation)dmlOperation;

                    var dmlExpr = new DmlInsertExpression(this, pe, insOp);
                    finalProjection = dmlExpr.GetProjection();
                    result = dmlExpr;
                    break;
                }
                default:
                    throw new InvalidOperationException($"Unknown DML operation: {dmlOperation.Kind}.");
                }
            }
            
            command.CommandText = result.ToString();
            
            PrepareResultTypes((NpgsqlCommand)command, finalProjection);
        }

        void BuildInsertFromObjectQuery(DbCommand command, DbDmlInsertOperation opInsert)
        {
            var insertColumnList = new StringBuilder(256);
            var selectColumnList = new StringBuilder(256);
            var mapInfo = opInsert.FromQueryMapping;
            var fromCommand = opInsert.FromStoreCommand;
            var entitySet = opInsert.TargetEntitySet;

            var nColumn = 0;
            
            foreach (var clm in entitySet.ElementType.Properties)
            {
                var mp = mapInfo.FirstOrDefault(e => e.TargetName == clm.Name);
                if (mp == null && IsNullable(clm.TypeUsage))
                    continue;
                
                if (nColumn > 0)
                {
                    insertColumnList.Append(", ");
                    selectColumnList.Append(", ");
                }
                insertColumnList.Append(QuoteIdentifier(clm.Name));
                if (mp == null)
                {
                    var sqlType = GetDbType(clm.TypeUsage.EdmType);
                    selectColumnList.Append("CAST(").Append(GetDefaultPrimitiveLiteral(clm.TypeUsage)).Append(" AS ").Append(sqlType).Append(")");
                }
                else
                {
                    selectColumnList.Append(QuoteIdentifier(mp.SourceProperty.Name));
                }
                selectColumnList.Append(" AS ").Append(QuoteIdentifier(clm.Name));
                nColumn++;
            }

            var sbCommand = new StringBuilder(512);
            
            if (opInsert.WithRowCount)
                sbCommand.Append("WITH ").Append("__cte_insert__").Append(" AS (").AppendLine();
            
            sbCommand.Append("INSERT" + " INTO ");

            if (!string.IsNullOrEmpty(entitySet.Schema))
                sbCommand.Append(QuoteIdentifier(entitySet.Schema)).Append(".");

            sbCommand.Append(QuoteIdentifier(entitySet.Table));
            
            sbCommand.AppendLine().Append('(').Append(insertColumnList).Append(')').AppendLine()
                .Append("SELECT ").Append(selectColumnList).Append(" FROM (");

            sbCommand.AppendLine().Append(fromCommand.CommandText)
                .AppendLine().Append(") AS x__subquery");

            if (opInsert.WithRowCount)
                sbCommand
                    .AppendLine()
                    .Append("    RETURNING 1").AppendLine()
                    .Append(")").AppendLine()
                    .Append("SELECT " + "count(1) FROM ").Append("__cte_insert__");

            command.CommandText = sbCommand.ToString();
        }

        static void PrepareResultTypes(NpgsqlCommand command, CommaSeparatedExpression projection)
        {
            var unknownResultTypeList = new bool[projection.Arguments.Count];
            Type[] objectResultTypes = null;

            for (var i = 0; i < projection.Arguments.Count; i++)
            {
                var a = projection.Arguments[i];
                unknownResultTypeList[i] = ((PrimitiveType)((ColumnExpression)a).ColumnType.EdmType).PrimitiveTypeKind == PrimitiveTypeKind.String;
                var kind = ((PrimitiveType)((ColumnExpression)a).ColumnType.EdmType).PrimitiveTypeKind;

                switch (kind)
                {
                    case PrimitiveTypeKind.SByte:
                        SetResultType(typeof(sbyte));
                        break;
                    case PrimitiveTypeKind.DateTimeOffset:
                        SetResultType(typeof(DateTimeOffset));
                        break;
                }

                void SetResultType(Type type)
                {
                    objectResultTypes ??= new Type[projection.Arguments.Count];
                    objectResultTypes[i] = type;
                }
            }

            command.UnknownResultTypeList = unknownResultTypeList;
            command.ObjectResultTypes = objectResultTypes;
        }
        
        private static string GetDefaultPrimitiveLiteral(TypeUsage storeTypeUsage)
        {
            Debug.Assert(BuiltInTypeKind.PrimitiveType == storeTypeUsage.EdmType.BuiltInTypeKind, "Type must be primitive type");

            var primitiveTypeKind = GetPrimitiveTypeKind(storeTypeUsage);
            
            switch (primitiveTypeKind)
            {
            case PrimitiveTypeKind.Byte:
            case PrimitiveTypeKind.Decimal:
            case PrimitiveTypeKind.Boolean:
            case PrimitiveTypeKind.Double:
            case PrimitiveTypeKind.Single:
            case PrimitiveTypeKind.SByte:
            case PrimitiveTypeKind.Int16:
            case PrimitiveTypeKind.Int32:
            case PrimitiveTypeKind.Int64:
                return "0";
            case PrimitiveTypeKind.Binary:
                return "E''";
            case PrimitiveTypeKind.DateTime:
                return "'00010101'";
            case PrimitiveTypeKind.Guid:
                return "'00000000-0000-0000-0000-000000000000'";
            case PrimitiveTypeKind.String:
                return "''";
            case PrimitiveTypeKind.Time:
                return "'0'";
            case PrimitiveTypeKind.DateTimeOffset:
                return "'00010101'";
            default:
                throw new InvalidOperationException($"PrimitiveTypeKind {primitiveTypeKind} is not supported.");
            }
        }
    }
}
