package com.hortonworks.spark.sql.hive.llap

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
