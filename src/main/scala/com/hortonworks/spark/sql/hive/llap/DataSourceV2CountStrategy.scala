package com.hortonworks.spark.sql.hive.llap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.types.{DataType, StructType}


case class DataSourceV2CountStrategy(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case Aggregate(groupingExpressions,
    Alias(AggregateExpression(af, _, _, _) , _) :: Nil,
    dsr @ DataSourceV2Relation(fullOutput, reader)) => {
      if(reader.isInstanceOf[HiveWarehouseDataSourceReader]) {
        val hiveReader = reader.asInstanceOf[HiveWarehouseDataSourceReader];
        hiveReader.options.put("isCountStar", "true");
        dsr
      } else {
        plan
      }
    }
    case _ => plan
  }
}
