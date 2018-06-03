/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap

import java.net.URI
import java.sql.{Connection, DatabaseMetaData, Driver, DriverManager, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbcp2.BasicDataSourceFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.typeinfo._
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


object Utils {
  def classForName(className: String): Class[_] = {
    // scalastyle:off classforname
    Class.forName(
      className,
      true,
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))
    // scalastyle:on classforname
  }
}

object DefaultJDBCWrapper extends JDBCWrapper

/**
 * Shim which exposes some JDBC helper functions. Most of this code is copied from Spark SQL, with
 * minor modifications for Redshift-specific features and limitations.
 */
class JDBCWrapper {

  private val log = LoggerFactory.getLogger(getClass)

  private val connectionPools = new scala.collection.mutable.HashMap[String, BasicDataSource]

  /**
   * Given a JDBC subprotocol, returns the appropriate driver class so that it can be registered
   * with Spark. If the user has explicitly specified a driver class in their configuration then
   * that class will be used. Otherwise, we will attempt to load the correct driver class based on
   * the JDBC subprotocol.
   *
   * @param jdbcSubprotocol 'redshift' or 'postgres'
   * @param userProvidedDriverClass an optional user-provided explicit driver class name
   * @return the driver class
   */
  private def getDriverClass(
      jdbcSubprotocol: String,
      userProvidedDriverClass: Option[String]): Class[Driver] = {
    userProvidedDriverClass.map(Utils.classForName).getOrElse {
      jdbcSubprotocol match {
        case "hive2" =>
          try {
            Utils.classForName("org.apache.hive.jdbc.HiveDriver")
          } catch {
            case _: ClassNotFoundException =>
              try {
                Utils.classForName("org.apache.hive.jdbc.HiveDriver")
              } catch {
                case e: ClassNotFoundException =>
                  throw new ClassNotFoundException(
                    "Could not load Hive JDBC driver.", e)
              }
          }
        case other => throw new IllegalArgumentException(s"Unsupported JDBC protocol: '$other'")
      }
    }.asInstanceOf[Class[Driver]]
  }

  private def registerDriver(driverClass: String): Unit = {
    val className = "org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry"
    val driverRegistryClass = Utils.classForName(className)
    val registerMethod = driverRegistryClass.getDeclaredMethod("register", classOf[String])
    registerMethod.invoke(null, driverClass)
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param conn A JDBC connection to the database.
   * @param dbName The database name.
   * @param tableName The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   *
   * @return A StructType giving the table's Catalyst schema.
   */
  def resolveTable(conn: Connection, dbName: String, tableName: String): StructType = {
    log.debug(s"resolveTable $dbName $tableName")
    val dbmd: DatabaseMetaData = conn.getMetaData()
    val rs: ResultSet = dbmd.getColumns(null, dbName, tableName, null)
    try {
      val fields = new ArrayBuffer[StructField]
      while (rs.next()) {
        val columnName = rs.getString(4)
        val dataType = rs.getInt(5)
        val typeName = rs.getString(6)
        val fieldSize = rs.getInt(7)
        val fieldScale = rs.getInt(9)
        val nullable = true // Hive cols nullable
        val isSigned = true
        val columnType = getCatalystType(dataType, typeName, fieldSize, fieldScale, isSigned)
        fields += StructField(columnName, columnType, nullable)
      }
      new StructType(fields.toArray)
    } finally {
      rs.close()
    }
  }

  def populateSchemaFields(ncols: Int,
                           rsmd: ResultSetMetaData,
                           fields: Array[StructField]): Unit = {
    var i = 0
    while (i < ncols) {
      // HIVE-11750 - ResultSetMetadata.getColumnName() has format tablename.columnname
      // Hack to remove the table name
      val columnName = rsmd.getColumnLabel(i + 1).split("\\.").last
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = true
      val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val columnType = getCatalystType(dataType, typeName, fieldSize, fieldScale, isSigned)
      fields(i) = StructField(columnName, columnType, nullable)
      i = i + 1
    }
  }

  // Used for executing queries directly from the Driver to HS2
  // ResultSet size is limited to prevent Driver OOM
  // Should not be used for processing of big data
  // Useful for DDL instrospection statements like 'show tables'
 def executeStmt(conn: Connection,
                 currentDatabase: String,
                 query: String,
                 maxExecResults: Long): DriverResultSet = {
    useDatabase(conn, currentDatabase)
    val stmt = conn.prepareStatement(query)
    stmt.setMaxRows(maxExecResults.asInstanceOf[Int])
    val rs = stmt.executeQuery()
    log.debug(query)
    try {
      val rsmd = rs.getMetaData
      val ncols = rsmd.getColumnCount
      val fields = new Array[StructField](ncols)
      populateSchemaFields(ncols, rsmd, fields)
      val schema = new StructType(fields)
      val data = new java.util.ArrayList[Row]()
      while(rs.next()) {
        val rowData = new Array[Any](ncols)
        for (j <- 0 to ncols - 1) {
          rowData(j) = rs.getObject(j + 1)
        }
        val row = new GenericRow(rowData)
        data.add(row)
      }
      return new DriverResultSet(data, schema)
    } finally {
      rs.close()
      stmt.close()
    }
  }

  // Used for executing statements directly from the Driver to HS2
  // with no results
  // Useful for DDL statements like 'create table'
  def executeUpdate(conn: Connection,
                  currentDatabase: String,
                  query: String): Boolean = {
    useDatabase(conn, currentDatabase)
    val stmt = conn.prepareStatement(query)
    val succeed = stmt.execute()
    stmt.close()
    log.debug(query)
    succeed
  }

  def useDatabase(conn: Connection, currentDatabase: String) {
    if (currentDatabase != null) {
      val stmt = conn.prepareStatement(s"USE $currentDatabase")
      stmt.execute()
      stmt.close()
    }
  }

  def resolveQuery(conn: Connection, currentDatabase: String, query: String): StructType = {
    useDatabase(conn, currentDatabase)
    val schemaQuery = s"SELECT * FROM ($query) q LIMIT 0"
    val rs = conn.prepareStatement(schemaQuery).executeQuery()
    log.debug(schemaQuery)
    try {
      val rsmd = rs.getMetaData
      val ncols = rsmd.getColumnCount
      val fields = new Array[StructField](ncols)
      populateSchemaFields(ncols, rsmd, fields)
      new StructType(fields)
    } finally {
      rs.close()
    }
  }

  /**
   * Given a driver string and a JDBC url, load the specified driver and return a DB connection.
   *
   * @param userProvidedDriverClass the class name of the JDBC driver for the given url. If this
   *                                is None then `spark-redshift` will attempt to automatically
   *                                discover the appropriate driver class.
   * @param url the JDBC url to connect to.
   */
  def getConnector(
      userProvidedDriverClass: Option[String],
      url: String,
      userName: String,
      dbcp2Configs: String): Connection = {
    log.debug(s"${userProvidedDriverClass.getOrElse("")} $url $userName password")
    val subprotocol = new URI(url.stripPrefix("jdbc:")).getScheme
    val driverClass: Class[Driver] = getDriverClass(subprotocol, userProvidedDriverClass)

    connectionPools.get(userName) match {
      case Some(d) =>
        d.getConnection
      case None =>
        val properties = new Properties()
        // for older version of HDP which do not have dbcp2 configurations
        if (dbcp2Configs == null) {
          properties.setProperty("initialSize", "1")
          properties.setProperty("maxConnLifetimeMillis", "100000")
          properties.setProperty("maxTotal", "40")
          properties.setProperty("maxIdle", "10")
          properties.setProperty("maxWaitMillis", "30000")
          properties.setProperty("logExpiredConnections", "false")
        } else {
          dbcp2Configs.split(" ").map(s => s.trim.split(":")).foreach {
            conf =>
              properties.setProperty(conf(0), conf(1))
              log.debug(conf(0) + ":" + conf(1))
          }
        }
        properties.setProperty("driverClassName", driverClass.getCanonicalName)
        properties.setProperty("url", url)
        properties.setProperty("password", "password")
        properties.setProperty("username", userName)
        val dataSource = BasicDataSourceFactory.createDataSource(properties)
        connectionPools.put(userName, dataSource)
        dataSource.getConnection
    }
  }

  def getConnector(sessionState: HiveWarehouseSessionState): Connection = {
    return getConnector(
      Option.empty,
      HWConf.RESOLVED_HS2_URL.getString(sessionState),
      HWConf.USER.getString(sessionState),
      HWConf.DBCP2_CONF.getString(sessionState)
    )
  }

  def columnString(dataType: DataType, dataSize: Option[Long]): String = dataType match {
    case IntegerType => "INTEGER"
    case LongType => "BIGINT"
    case DoubleType => "DOUBLE"
    case FloatType => "REAL"
    case ShortType => "SMALLINT"
    case ByteType => "TINYINT"
    case BooleanType => "BOOLEAN"
    case StringType =>
      if (dataSize.isDefined) {
        s"VARCHAR(${dataSize.get})"
      } else {
        "TEXT"
      }
    case BinaryType => "BLOB"
    case TimestampType => "TIMESTAMP"
    case DateType => "DATE"
    case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
    case _ => throw new IllegalArgumentException(s"Don't know how to save $dataType to JDBC")
  }

  /**
   * Compute the SQL schema string for the given Spark SQL Schema.
   */
  def schemaString(schema: StructType): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field => {
      val name = field.name
      val size = if (field.metadata.contains("maxLength")) {
        Some(field.metadata.getLong("maxLength"))
      } else {
        None
      }
      val typ: String = columnString(field.dataType, size)
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s""", "${name.replace("\"", "\\\"")}" $typ $nullable""".trim)
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Maps a JDBC type to a Catalyst type.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  def getCatalystType(
      sqlType: Int,
      typeName: String,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    log.debug(s"getCatalystType $sqlType")
    // For primitive types, we can just use the sqlType passed in.
    // Complex types require use of the typeName.
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.TINYINT       => ByteType
      case java.sql.Types.SMALLINT      => ShortType
      case java.sql.Types.INTEGER       => IntegerType
      case java.sql.Types.BIGINT        => LongType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.VARCHAR       => StringType
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.DECIMAL       => DecimalType(precision, scale)
      case java.sql.Types.ARRAY         => getCatalystType(typeName)
      case java.sql.Types.STRUCT        => getCatalystType(typeName)
      case java.sql.Types.JAVA_OBJECT
        if (typeName.toLowerCase().startsWith("map")) => getCatalystType(typeName)
      case _                            => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  private def getCatalystType(typeInfo: TypeInfo) : DataType = {
    typeInfo.getCategory match {
      case Category.PRIMITIVE =>
        getCatalystType(typeInfo.asInstanceOf[PrimitiveTypeInfo])
      case Category.LIST =>
        ArrayType(getCatalystType(typeInfo.asInstanceOf[ListTypeInfo].getListElementTypeInfo))
      case Category.MAP =>
        MapType(
          getCatalystType(typeInfo.asInstanceOf[MapTypeInfo].getMapKeyTypeInfo),
          getCatalystType(typeInfo.asInstanceOf[MapTypeInfo].getMapValueTypeInfo))
      case Category.STRUCT =>
        StructType(getCatalystStructFields(typeInfo.asInstanceOf[StructTypeInfo]))
      case _ =>
        throw new SQLException("Unsupported type " + typeInfo)
    }
  }

  private def getCatalystType(primitiveTypeInfo: PrimitiveTypeInfo) : DataType = {
    primitiveTypeInfo.getPrimitiveCategory match {
      case PrimitiveCategory.BOOLEAN => BooleanType
      case PrimitiveCategory.BYTE => ByteType
      case PrimitiveCategory.SHORT => ShortType
      case PrimitiveCategory.INT => IntegerType
      case PrimitiveCategory.LONG => LongType
      case PrimitiveCategory.FLOAT => FloatType
      case PrimitiveCategory.DOUBLE => DoubleType
      case PrimitiveCategory.STRING => StringType
      case PrimitiveCategory.CHAR => StringType
      case PrimitiveCategory.VARCHAR => StringType
      case PrimitiveCategory.DATE => DateType
      case PrimitiveCategory.TIMESTAMP => TimestampType
      case PrimitiveCategory.BINARY => BinaryType
      case PrimitiveCategory.DECIMAL => DecimalType(
          primitiveTypeInfo.asInstanceOf[DecimalTypeInfo].getPrecision,
          primitiveTypeInfo.asInstanceOf[DecimalTypeInfo].getScale)
      case _ => throw new SQLException("Unsupported type " + primitiveTypeInfo)
    }
  }

  private def getCatalystStructFields(structTypeInfo: StructTypeInfo) : Array[StructField] = {
    structTypeInfo.getAllStructFieldNames.asScala
      .zip(structTypeInfo.getAllStructFieldTypeInfos.asScala).map(
        { case (fieldName, fieldType) => new StructField(fieldName, getCatalystType(fieldType)) }
      ).toArray
  }

  private def getCatalystType(typeName: String) : DataType = {
    getCatalystType(TypeInfoUtils.getTypeInfoFromTypeString(typeName))
  }
}
