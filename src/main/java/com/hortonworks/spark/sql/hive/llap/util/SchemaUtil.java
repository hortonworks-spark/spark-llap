package com.hortonworks.spark.sql.hive.llap.util;

import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class SchemaUtil {

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

  public static TableRef getDbTableNames(String nameStr) {
    String[] nameParts = nameStr.split("\\.");
    if (nameParts.length != 2) {
      throw new IllegalArgumentException("Expected " + nameStr + " to be in the form db.table");
    }
    return new TableRef(nameParts[0], nameParts[1]);
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
