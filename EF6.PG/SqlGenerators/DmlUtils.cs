using System;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data.Entity.Core.Metadata.Edm;
using System.Text;

namespace Npgsql.SqlGenerators
{
    internal static class DmlUtils
    {
        public const string CtidColumnName = "CTID";
        public const string CtidAlias = "__internal_row_id__";
    }

    internal static class PrimitiveTypeUsages
    {
        public static readonly TypeUsage Int32 = TypeUsage.Create(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int32), Array.Empty<Facet>());
    }
    
    internal class FindDmlTargetVisitor
    {
        public FindDmlTargetVisitor(EntitySet target)
        {
            Target = target;
        }

        EntitySet Target { get; }

        ScanExpression TargetScan { get; set; }
        public FromExpression TargetFrom { get; private set; }

        InputExpression _initialInput;
        bool _isProjected;
        bool _addColumnToInitialInput;

        public string ColumnName { get; private set; }
        public string TableName { get; private set; }
        public bool Success => ColumnName != null;
        
        public void PullUpCtid(InputExpression inputExpression, bool addColumnToInitialInput)
        {
            _initialInput = inputExpression;
            _addColumnToInitialInput = addColumnToInitialInput;
            VisitInput(inputExpression, null);
        }

        void VisitInput(InputExpression inputExpression, FromExpression parentFrom)
        {
            VisitFrom(inputExpression.From, null);

            if (TargetScan != null)
            {
                if (_isProjected)
                {
                    AddProjectionColumn(inputExpression, TableName, DmlUtils.CtidAlias, DmlUtils.CtidAlias);
                    ColumnName = DmlUtils.CtidAlias;
                }
                else if (inputExpression == _initialInput)
                {
                    if (_addColumnToInitialInput)
                    {
                        _isProjected = true;
                        AddProjectionColumn(inputExpression, TableName, DmlUtils.CtidColumnName, DmlUtils.CtidAlias);
                        ColumnName = DmlUtils.CtidAlias;
                    }
                    else
                        ColumnName = DmlUtils.CtidColumnName;
                }
                else if (inputExpression != _initialInput)
                {
                    if (!_isProjected)
                    {
                        if (!(parentFrom != null && parentFrom.CanSkipSubquery()))
                        {
                            _isProjected = true;
                            AddProjectionColumn(inputExpression, TableName, DmlUtils.CtidColumnName, DmlUtils.CtidAlias);
                            ColumnName = DmlUtils.CtidAlias;
                        }
                    }
                }
            }
        }

        void AddProjectionColumn(InputExpression inputExpression, string projectionAlias, string columnName, string outputColumnAlias)
        {
            if (inputExpression.Projection != null)
            {
                if (!ReferenceEquals(inputExpression, _initialInput) || _addColumnToInitialInput)
                {
                    inputExpression.Projection.Arguments.Add(new ColumnExpression(
                        new ColumnReferenceExpression
                        {
                            Variable = projectionAlias,
                            Name = columnName,
                            QuoteName = (columnName != DmlUtils.CtidColumnName)
                        }, outputColumnAlias, PrimitiveTypeUsages.Int32));
                }
            }
            else
            {
                inputExpression.ColumnsToProject.Add(new StringPair(projectionAlias, columnName), outputColumnAlias);
            }
        }

        void VisitFrom(VisitedExpression fromExpression, FromExpression parentFrom)
        {
            switch (fromExpression)
            {
                case JoinExpression join:
                    VisitFrom(join.Left, null);
                    if (TargetScan == null)
                    {
                        VisitFrom(join.Right, null);
                    }

                    break;
                case FromExpression from:
                    VisitFrom(from.From, from);
                    if (TargetScan != null)
                    {
                        TableName = from.Name;
                        TargetFrom ??= from;
                    }

                    break;
                case InputExpression input:
                    VisitInput(input, parentFrom);
                    break;
                case ScanExpression scan:
                    if (scan.Target == Target)
                    {
                        TargetScan = scan;
                        ColumnName = TargetScan.ScanString;
                    }
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported From expression: {fromExpression.GetType().Name}.");
            }
        }
    }

    internal abstract class DmlExpressionBase : VisitedExpression
    {
        static readonly CommaSeparatedExpression RowCountProjection;

        static DmlExpressionBase()
        {
            RowCountProjection = new CommaSeparatedExpression();
            RowCountProjection.Arguments.Add(
                new ColumnExpression(new LiteralExpression("1"), "C1", PrimitiveTypeUsages.Int32)
                );
        }

        protected abstract bool WithRowCount { get; }
        
        protected abstract string CteRowCountName { get; }
        
        internal sealed override void WriteSql(StringBuilder sqlText)
        {
            if (WithRowCount)
            {
                sqlText.Append("WITH ").Append(CteRowCountName).Append(" AS (").AppendLine();
            }
            
            WriteDmlCommand(sqlText);

            if (WithRowCount)
            {
                sqlText
                    .AppendLine()
                    .Append("    RETURNING 1").AppendLine()
                    .Append(")").AppendLine()
                    .Append("SELECT " + "count(1) FROM ").Append(CteRowCountName);
            }
        }
        
        public CommaSeparatedExpression GetProjection()
        {
            return RowCountProjection;
        }

        protected abstract void WriteDmlCommand(StringBuilder sqlText);

        // returns true if simple construction
        protected static bool AnalyzeFrom(DbDmlOperation dmlOp, InputExpression inputExpression, out FromExpression target, out VisitedExpression fromRevisited, out VisitedExpression joinCondition)
        {
            fromRevisited = joinCondition = null;

            if (inputExpression.Limit != null || inputExpression.Skip != null)
            {
                target = null;
                return false;
            }

            switch (inputExpression.From)
            {
            case FromExpression fromExp:
                if (fromExp.From is ScanExpression scan && scan.Target == dmlOp.TargetEntitySet)
                {
                    target = fromExp;
                    return true;
                }

                break;
            case JoinExpression joinExp:
                if (joinExp.JoinType == DbExpressionKind.InnerJoin)
                {
                    if (AnalyzeJoinPart(joinExp.Left, out var joinFrom))
                    {
                        fromRevisited = joinExp.Right;
                        joinCondition = joinExp.Condition;
                        target = joinFrom;
                        return true;
                    }

                    if (AnalyzeJoinPart(joinExp.Right, out joinFrom))
                    {
                        fromRevisited = joinExp.Left;
                        joinCondition = joinExp.Condition;
                        target = joinFrom;
                        return true;
                    }
                }

                break;
            }

            target = null;
            return false;

            // ===================================================================================
            
            bool AnalyzeJoinPart(VisitedExpression joinFrom, out FromExpression from)
            {
                if (joinFrom is FromExpression
                {
                    From: InputExpression
                    {
                        From: FromExpression { From: ScanExpression fScan },
                        Where: null,
                        Distinct: false,
                        Limit: null,
                        Projection: null,
                        Skip: null,
                        GroupBy: null,
                        OrderBy: null
                    }
                } f && fScan.Target == dmlOp.TargetEntitySet)
                {
                    from = f;
                    return true;
                }

                from = null;
                return false;
            }
        }
        
    }

    internal sealed class DmlDeleteExpression : DmlExpressionBase
    {
        const string DeleteTargetName = "__DELETE_TARGET__";
        const string DeleteSourceName = "__DELETE_SOURCE__";
        
        protected override bool WithRowCount => DeleteOp.WithRowCount;
        
        protected override string CteRowCountName => "__cte_delete__";
        
        InputExpression InputExpression { get; }
        
        DbDmlDeleteOperation DeleteOp { get; }

        FromExpression Target { get; set; }

        bool _isSimpleForm;
        
        public DmlDeleteExpression(InputExpression inputExpression, DbDmlDeleteOperation deleteOp)
        {
            InputExpression = inputExpression; 
            DeleteOp = deleteOp;
            
            AnalyzeInputExpression();
        }
        
        void AnalyzeInputExpression()
        {
            if (InputExpression.OrderBy != null)
                throw new InvalidOperationException("OrderBy is not supported in Delete queries.");
            if (InputExpression.GroupBy != null)
                throw new InvalidOperationException("GroupBy is not supported in Delete queries.");
            if (InputExpression.Distinct)
                throw new InvalidOperationException("Distinct is not supported in Delete queries.");

            if (DeleteOp.Limit >= 0)
            {
                if (InputExpression.Limit != null || InputExpression.Skip != null)
                    throw new InvalidOperationException("Take / Skip is not supported in Updatable queries.");
                InputExpression.Limit = new LimitExpression(new LiteralExpression(DeleteOp.Limit.ToString()));
            }

            _isSimpleForm = AnalyzeFrom(DeleteOp, InputExpression, out var target, out var fromRevisited, out var joinCondition);
            if (_isSimpleForm)
            {
                InputExpression.From = fromRevisited;

                if (joinCondition != null)
                {
                    var where = new WhereExpression(joinCondition);
                    if (InputExpression.Where != null)
                        where.And(InputExpression.Where.Condition);
                    InputExpression.Where = where;
                }

                Target = target;
            }
            else
            {
                // Find target Extent for subquery
                var dmlTarget = new FindDmlTargetVisitor(DeleteOp.TargetEntitySet);
                dmlTarget.PullUpCtid(InputExpression, true);

                if (!dmlTarget.Success)
                    throw new InvalidOperationException($"Can't project CTID column for query {InputExpression}.");

                Target = dmlTarget.TargetFrom;
            }
        }

        protected override void WriteDmlCommand(StringBuilder sqlText)
        {
            if (!_isSimpleForm)
                WriteSubquerySql(sqlText);
            else
                WriteSimpleSql(sqlText);
        }

        void WriteSimpleSql(StringBuilder sqlText)
        {
            sqlText.Append("DELETE " + "FROM ");
            Target.WriteSql(sqlText);
            sqlText.AppendLine();

            if (InputExpression.From != null)
            {
                sqlText.Append("USING ");
                InputExpression.From.WriteSql(sqlText);
                sqlText.AppendLine();
            }
            
            InputExpression.Where?.WriteSql(sqlText);
        }

        void WriteSubquerySql(StringBuilder sqlText)
        {
            sqlText.Append("DELETE " + "FROM ");
            var target = new FromExpression(Target.From, DeleteTargetName);
            target.WriteSql(sqlText);
            sqlText.AppendLine();
            sqlText.Append("USING (").AppendLine();
            InputExpression.WriteSql(sqlText);
            sqlText.AppendLine().Append("FOR UPDATE");
            sqlText.AppendLine().Append(") AS ").Append(SqlBaseGenerator.QuoteIdentifier(DeleteSourceName))
                .AppendLine().Append("WHERE ")
                .Append(SqlBaseGenerator.QuoteIdentifier(DeleteTargetName)).Append(".").Append(DmlUtils.CtidColumnName)
                .Append(" = ")
                .Append(SqlBaseGenerator.QuoteIdentifier(DeleteSourceName)).Append(".").Append(SqlBaseGenerator.QuoteIdentifier(DmlUtils.CtidAlias));
        }
    }
    
    internal sealed class DmlUpdateExpression : DmlExpressionBase
    {
        const string UpdateTargetName = "__UPDATE_TARGET__";
        const string UpdateSourceName = "__UPDATE_SOURCE__";

        protected override string CteRowCountName => "__cte_update__"; 
        
        InputExpression InputExpression { get; }
        
        DbDmlUpdateOperation UpdateOp { get; }

        protected override bool WithRowCount => UpdateOp.WithRowCount;
        
        FromExpression Target { get; set; }

        bool _isSimpleForm;

        public DmlUpdateExpression(InputExpression inputExpression, DbDmlUpdateOperation updateOp)
        {
            UpdateOp = updateOp;
            InputExpression = inputExpression;
            
            AnalyzeInputExpression();
        }

        void AnalyzeInputExpression()
        {
            if (InputExpression.OrderBy != null)
                throw new InvalidOperationException("OrderBy is not supported in Updatable queries.");
            if (InputExpression.GroupBy != null)
                throw new InvalidOperationException("GroupBy is not supported in Updatable queries.");
            if (InputExpression.Distinct)
                throw new InvalidOperationException("Distinct is not supported in Updatable queries.");

            if (UpdateOp.Limit >= 0)
            {
                if (InputExpression.Limit != null || InputExpression.Skip != null)
                    throw new InvalidOperationException("Take / Skip is not supported in Updatable queries.");
                InputExpression.Limit = new LimitExpression(new LiteralExpression(UpdateOp.Limit.ToString()));
            }

            _isSimpleForm = AnalyzeFrom(UpdateOp, InputExpression, out var target, out var fromRevisited, out var joinCondition);
            if (_isSimpleForm)
            {
                InputExpression.From = fromRevisited;

                if (joinCondition != null)
                {
                    var where = new WhereExpression(joinCondition);
                    if (InputExpression.Where != null)
                        where.And(InputExpression.Where.Condition);
                    InputExpression.Where = where;
                }

                Target = target;
            }
            else
            {
                // Find target Extent for subquery
                var dmlTarget = new FindDmlTargetVisitor(UpdateOp.TargetEntitySet);
                dmlTarget.PullUpCtid(InputExpression, true);

                if (!dmlTarget.Success)
                    throw new InvalidOperationException($"Can't project CTID column for query {InputExpression}.");

                Target = dmlTarget.TargetFrom;
            }
        }

        protected override void WriteDmlCommand(StringBuilder sqlText)
        {
            if (!_isSimpleForm)
                WriteSubquerySql(sqlText);
            else
                WriteSimpleSql(sqlText);
        }

        void WriteSimpleSql(StringBuilder sqlText)
        {
            sqlText.Append("UPDATE ");
            Target.WriteSql(sqlText);
            WriteSetClause(sqlText);
            sqlText.AppendLine();

            if (InputExpression.From != null)
            {
                sqlText.Append("FROM ");
                InputExpression.From.WriteSql(sqlText);
                sqlText.AppendLine();
            }
            
            InputExpression.Where?.WriteSql(sqlText);
        }

        void WriteSubquerySql(StringBuilder sqlText)
        {
            sqlText.Append("UPDATE ");
            var target = new FromExpression(Target.From, UpdateTargetName);
            target.WriteSql(sqlText);
            WriteSetClause(sqlText);
            sqlText.AppendLine();
            sqlText.Append("FROM (").AppendLine();
            InputExpression.WriteSql(sqlText);
            sqlText.AppendLine().Append("FOR UPDATE");
            sqlText.AppendLine().Append(") AS ").Append(SqlBaseGenerator.QuoteIdentifier(UpdateSourceName))
                .AppendLine().Append("WHERE ")
                .Append(SqlBaseGenerator.QuoteIdentifier(UpdateTargetName)).Append(".").Append(DmlUtils.CtidColumnName)
                .Append(" = ")
                .Append(SqlBaseGenerator.QuoteIdentifier(UpdateSourceName)).Append(".").Append(SqlBaseGenerator.QuoteIdentifier(DmlUtils.CtidAlias));
        }

        void WriteSetClause(StringBuilder sqlText)
        {
            sqlText.AppendLine().Append("SET");
            var hasAnyColumn = false;
            for (var colIndex = 0; colIndex < InputExpression.Projection.Arguments.Count; colIndex++)
            {
                var map = FindMapping(colIndex);
                if (map.TargetName == null)
                {
                    if (colIndex == UpdateOp.ColumnMap.NullSentinelOrdinal
                        || InputExpression.Projection.Arguments[colIndex] is ColumnExpression { Name: DmlUtils.CtidAlias })
                        continue;
                    throw new InvalidOperationException($"Can't find map for column # {colIndex}.");
                }

                var colExp = InputExpression.Projection.Arguments[colIndex] as ColumnExpression
                             ?? throw new InvalidOperationException($"A ColumnExpression expected for column # {colIndex}.");

                if (hasAnyColumn)
                    sqlText.Append(",");

                sqlText.AppendLine()
                    .Append("    ").Append(SqlBaseGenerator.QuoteIdentifier(map.TargetName))
                    .Append(" = ");
                
                if (_isSimpleForm)
                    colExp.Column.WriteSql(sqlText);
                else
                    sqlText.Append(SqlBaseGenerator.QuoteIdentifier(UpdateSourceName)).Append(".").Append(SqlBaseGenerator.QuoteIdentifier(colExp.Name));

                hasAnyColumn = true;
            }
            
            if (!hasAnyColumn)
                throw new InvalidOperationException("No columns in Update.");
        }

        DmlColumnMap FindMapping(int projectionIndex)
        {
            var mappings = UpdateOp.ColumnMap.Mappings;
            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < mappings.Length; i++)
            {
                if (mappings[i].SourceOrdinal == projectionIndex)
                    return mappings[i];
            }

            return default;
        }
    }

    internal sealed class DmlInsertExpression : DmlExpressionBase
    {
        readonly SqlSelectGenerator _sqlSelectGenerator;
        protected override string CteRowCountName => "__cte_insert__"; 
        DbDmlInsertOperation InsertOp { get; }
        protected override bool WithRowCount => InsertOp.WithRowCount;
        InputExpression InputExpression { get; }
        
        public DmlInsertExpression(SqlSelectGenerator sqlSelectGenerator, InputExpression inputExpression, DbDmlInsertOperation insertOp)
        {
            _sqlSelectGenerator = sqlSelectGenerator;
            InsertOp = insertOp;
            InputExpression = inputExpression;
            SortOutputColumnsAccordingMappings();
        }

        void SortOutputColumnsAccordingMappings()
        {
            var mappings = InsertOp.ColumnMap.Mappings;
            var outColumns = new VisitedExpression[mappings.Length];
            
            for (var i = 0; i < InputExpression.Projection.Arguments.Count; i++)
            {
                var column = InputExpression.Projection.Arguments[i];
                var found = false;
                
                for (var iMap = 0; iMap < mappings.Length; iMap++)
                {
                    if (mappings[iMap].SourceOrdinal == i)
                    {
                        if (outColumns[iMap] != null)
                            throw new InvalidOperationException($"Column {mappings[iMap].TargetName} is mapped twice.");
                        outColumns[iMap] = column;
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    if (i == InsertOp.ColumnMap.NullSentinelOrdinal)
                        continue;
                    
                    throw new InvalidOperationException($"Can't find map for column # {i}.");
                }
            }
            
            InputExpression.Projection.Arguments.Clear();
            
            for (var i = 0; i < outColumns.Length; i++)
            {
                if (outColumns[i] == null)
                    throw new InvalidOperationException($"Can't find map for column {mappings[i].TargetName}.");
                InputExpression.Projection.Arguments.Add(outColumns[i]);
            }

            if (InsertOp.Discriminators != null && InsertOp.Discriminators.Length > 0)
            {
                var oldCreateParametersForConstants = _sqlSelectGenerator.CreateParametersForConstants;
                foreach (var discriminator in InsertOp.Discriminators)
                {
                    var discValue = discriminator.Column.TypeUsage.Constant(discriminator.Value);
                    _sqlSelectGenerator.CreateParametersForConstants = false;
                    InputExpression.Projection.Arguments.Add(
                        new ColumnExpression(
                            _sqlSelectGenerator.Visit(discValue),
                            "__discriminator__" + discriminator.Column.Name,
                            discriminator.Column.TypeUsage)
                    );
                }
                _sqlSelectGenerator.CreateParametersForConstants = oldCreateParametersForConstants;
            }
        }

        protected override void WriteDmlCommand(StringBuilder sqlText)
        {
            sqlText.Append("INSERT" + " INTO ")
                .Append(SqlBaseGenerator.QuoteIdentifier(InsertOp.TargetEntitySet.Schema))
                .Append(".")
                .Append(SqlBaseGenerator.QuoteIdentifier(InsertOp.TargetEntitySet.Table))
                .Append(" (");

            if (InsertOp.ColumnMap == null || InsertOp.ColumnMap.Mappings.Length == 0)
                throw new InvalidOperationException("Column mapping is not set for Insert operation.");

            for (var i = 0; i < InsertOp.ColumnMap.Mappings.Length; i++)
            {
                if (i > 0)
                    sqlText.Append(", ");
                
                sqlText.Append(SqlBaseGenerator.QuoteIdentifier(InsertOp.ColumnMap.Mappings[i].TargetName));
            }
            
            if (InsertOp.Discriminators != null && InsertOp.Discriminators.Length > 0)
            {
                foreach (var discriminator in InsertOp.Discriminators)
                {
                    sqlText.Append(", ")
                        .Append(SqlBaseGenerator.QuoteIdentifier(discriminator.Column.Name));
                }
            }

            sqlText.Append(")");
            sqlText.AppendLine();
            
            InputExpression.WriteSql(sqlText);
        }
    }
}
