﻿using System.Collections.Generic;

namespace Npgsql.SqlGenerators
{
    /// <summary>
    /// Represents an InputExpression and what alias it will have when used in a FROM clause
    /// </summary>
    internal class NameAndInputExpression
    {
        public string AsName { get; }
        public InputExpression Exp { get; }
        
        public NameAndInputExpression(string asName, InputExpression exp)
        {
            AsName = asName;
            Exp = exp;
        }
    }

    /// <summary>
    /// A tree of subqueries, used when evaluating SQL text for DbPropertyExpressions in SqlSelectGenerator.
    /// See SqlSelectGenerator.Visit(DbPropertyExpression) for more information.
    /// </summary>
    internal class PendingProjectsNode
    {
        public readonly List<NameAndInputExpression> Selects = new List<NameAndInputExpression>();
        public PendingProjectsNode JoinParent { get; set; }
        public string TopName => Selects[0].AsName;

        public PendingProjectsNode(string asName, InputExpression exp)
        {
            Selects.Add(new NameAndInputExpression(asName, exp));
        }

        public void Add(string asName, InputExpression exp)
        {
            Selects.Add(new NameAndInputExpression(asName, exp));
        }

        public NameAndInputExpression Last => Selects[Selects.Count - 1];
    }
}
