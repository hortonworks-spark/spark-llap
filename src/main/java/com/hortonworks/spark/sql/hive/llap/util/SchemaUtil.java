package com.hortonworks.spark.sql.hive.llap.util;

import com.hortonworks.spark.sql.hive.llap.CreateTableBuilder;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class SchemaUtil {

  private static final String HIVE_TYPE_STRING = "HIVE_TYPE_STRING";

  public static StructType convertSchema(Schema schema) {
    List<FieldDesc> columns = schema.getColumns();
    List<String> types = new ArrayList<>();
    for(FieldDesc fieldDesc : columns) {
      String name;
      if(fieldDesc.getName().contains(".")) {
        name = fieldDesc.getName().split("\\.")[1];
      } else {
        name = fieldDesc.getName();
      }
      types.add(format("`%s` %s", name, fieldDesc.getTypeInfo().toString()));
    }
    return StructType.fromDDL(String.join(", ", types));
  }

  public static String[] columnNames(StructType schema) {
    String[] requiredColumns = new String[schema.length()];
    int i = 0;
    for (StructField field : schema.fields()) {
      requiredColumns[i] = field.name();
      i++;
    }
    return requiredColumns;
  }

  /**
   *
   * Builds create table query from given dataframe schema.
   *
   * @param schema  spark dataframe schema
   * @param database database name for create table query
   * @param table table name for create table query
   * @return a create table query string
   */
  public static String buildHiveCreateTableQueryFromSparkDFSchema(StructType schema, String database, String table) {
    CreateTableBuilder createTableBuilder = new CreateTableBuilder(null, database, table);
    for (StructField field : schema.fields()) {
      createTableBuilder.column(field.name(), getHiveType(field.dataType(), field.metadata()));
    }
    return createTableBuilder.toString();
  }

  /**
   * Converts a spark type to equivalent hive type.
   * <br/>
   * List of currently supported types: https://github.com/hortonworks-spark/spark-llap/tree/master#supported-types
   *
   * @param dataType spark data type
   * @param metadata metadata associated with related column
   * @return String representing equivalent hive type
   */
  public static String getHiveType(DataType dataType, Metadata metadata) {
    if (dataType instanceof MapType || dataType instanceof CalendarIntervalType || dataType instanceof NullType) {
      throw new IllegalArgumentException("Data type not supported currently: " + dataType);
    } else {
      String hiveDataType = dataType.catalogString();

      //for char and varchar types
      if (dataType instanceof StringType && metadata != null && metadata.contains(HIVE_TYPE_STRING)) {
        hiveDataType = metadata.getString(HIVE_TYPE_STRING);
      }
      return hiveDataType;
    }
  }

  public static TableRef getDbTableNames(String db, String nameStr) {
    String[] nameParts = nameStr.split("\\.");
    if (nameParts.length == 1) {
      //hive.table(<unqualified_tableName>) so fill in db from default session db
      return new TableRef(db, nameStr);
    } else if(nameParts.length == 2) {
      //hive.table(<qualified_tableName>) so use the provided db
      return new TableRef(nameParts[0], nameParts[1]);
    } else {
      throw new IllegalArgumentException("Table name should be specified as either <table> or <db.table>");
    }
  }

  public static class TableRef {
    public String databaseName;
    public String tableName;

    public TableRef(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }
  }
}
