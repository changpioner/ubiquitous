package com.github.ubiquitous.spark.util

import java.io.IOException
import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Namhwik on 2018/7/31.
  */

object HbaseCacheUtil {


  //1.获得Configuration实例并进行相关设置//1.获得Configuration实例并进行相关设置

  val configuration: Configuration = HBaseConfiguration.create
  //  configuration.set("hbase.zookeeper.quorum", "myhbase")
  //  configuration.set("hbase.zookeeper.property.clientPort","2181")
  //  configuration.set("hbase.master", "localhost:16010")
  //2.获得Connection实例
  val connection: Connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(configuration)
  //3.1获得Admin接口
  val admin: Admin = connection.getAdmin

  def listTables(): Unit = {
    admin.listTableNames().foreach(println(_))
  }

  /**
    * 创建表
    *
    * @param tableName   表名
    * @param familyNames 列族名
    **/
  @throws[IOException]
  def createTable(tableName: String, familyNames: String*): Unit = {
    if (admin.tableExists(TableName.valueOf(tableName))) return
    //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for (familyName <- familyNames) {
      tableDescriptor.addFamily(new HColumnDescriptor(familyName))
    }
    admin.createTable(tableDescriptor)
  }

  /**
    * 删除表
    *
    * @param tableName 表名
    **/
  @throws[IOException]
  def dropTable(tableName: String): Unit = {
    //删除之前要将表disable
    if (!admin.isTableDisabled(TableName.valueOf(tableName))) admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
  }


  /**
    * 指定行/列中插入数据
    *
    * @param tableName 表名
    * @param rowKey    主键rowkey
    * @param family    列族
    * @param column    列
    * @param value     值
    *                  TODO: 批量PUT
    */
  @throws[IOException]
  def insert(tableName: String, rowKey: String, family: String, column: String, value: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }


  def insert(tableName: String, rowKey: String, family: String, values: JSONObject, dtypes: Map[String, String] = Map[String, String]()): Unit = {
    val convert2Bytes = (tp: String, v: AnyRef) =>
      tp match {
        case "IntegerType" => Bytes.toBytes(v.asInstanceOf[Int])
        case "TimestampType" => Bytes.toBytes(v.toString)
        case "StringType" => Bytes.toBytes(v.asInstanceOf[String])
        case "LongType" => Bytes.toBytes(v.asInstanceOf[Long])
        case "BooleanType" => Bytes.toBytes(v.asInstanceOf[Boolean])
        case "DecimalType" => Bytes.toBytes(v.asInstanceOf[java.math.BigDecimal])
        case "FloatType" => Bytes.toBytes(v.asInstanceOf[Float])
        case "ShortType" => Bytes.toBytes(v.asInstanceOf[Short])
        case "DoubleType" => Bytes.toBytes(v.asInstanceOf[Double])
        case _ => Bytes.toBytes(v.toString)
      }
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    values.keySet().toList.foreach(column =>
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column),
        convert2Bytes(dtypes.getOrElse(column, "some_else"), values.get(column))
      )
    )
    table.put(put)
  }

  def convert2Bytes: Any => Array[Byte] = {
    case x: Int => Bytes.toBytes(x)
    case x: Timestamp => Bytes.toBytes(x.toString)
    case x: String => Bytes.toBytes(x)
    case x: Long => Bytes.toBytes(x)
    case x: Boolean => Bytes.toBytes(x)
    case x: java.math.BigDecimal => Bytes.toBytes(x)
    case x: Float => Bytes.toBytes(x)
    case x: Short => Bytes.toBytes(x)
    case x: Double => Bytes.toBytes(x)
    case v => Bytes.toBytes(v.toString)
  }

  def insertV2(tableName: String, rowKey: Any, family: String, values: Map[String, Any]): Unit = {

    connection.getTable(TableName.valueOf(tableName))
      .put(
        values.foldLeft(new Put(convert2Bytes(rowKey)))(
          (p: Put, kv: (String, Any)) =>
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(kv._1), convert2Bytes(kv._2))
        ))
  }

  /**
    * 只对常见类型进行了测试
    *
    * @param tableName
    * @param rowKey
    * @param family
    * @param dtypes
    * @return
    */
  def get(tableName: String, rowKey: Any, family: String, dtypes: Map[String, String]): Map[String, Any] = {

    val keyBytes = rowKey match {
      case _: Boolean => Bytes.toBytes(rowKey.asInstanceOf[Boolean])
      case _: Long => Bytes.toBytes(rowKey.asInstanceOf[Long])
      case _: Int => Bytes.toBytes(rowKey.asInstanceOf[Int])
      case _: java.math.BigDecimal => Bytes.toBytes(rowKey.asInstanceOf[java.math.BigDecimal])
      case _: Double => Bytes.toBytes(rowKey.asInstanceOf[Double])
      case _: String => Bytes.toBytes(rowKey.asInstanceOf[String])
      case _: Short => Bytes.toBytes(rowKey.asInstanceOf[Short])
      case _: ByteBuffer => Bytes.toBytes(rowKey.asInstanceOf[ByteBuffer])
      case _: Float => Bytes.toBytes(rowKey.asInstanceOf[Float])
      case _: java.sql.Timestamp => Bytes.toBytes(rowKey.toString)
    }
    val convert2Value = (tp: String, v: Array[Byte]) =>
      tp match {
        case "IntegerType" => Bytes.toInt(v)
        case "TimestampType" => Timestamp.valueOf(Bytes.toString(v))
        case "StringType" => Bytes.toString(v)
        case "LongType" => Bytes.toLong(v)
        case "BooleanType" => Bytes.toBoolean(v)
        case "DecimalType" => Bytes.toBigDecimal(v)
        case "FloatType" => Bytes.toFloat(v)
        case "ShortType" => Bytes.toShort(v)
        case "DoubleType" => Bytes.toDouble(v)
        case "ArrayType" => Bytes.toByteArrays(v)
        case _ => Bytes.toString(v)
      }
    rowKey match {
      case s@"" =>
        Map[String, String]()
      case _ =>
        val table = connection.getTable(TableName.valueOf(tableName))
        val get = new Get(keyBytes)
        if (family != null) {
          val familyBytes = Bytes.toBytes(family)
          get.addFamily(familyBytes)
          if (dtypes.nonEmpty)
            dtypes.foreach(t => get.addColumn(familyBytes, Bytes.toBytes(t._1)))
          val result: Result = table.get(get)
          dtypes.map(x => {
            val valueBytes: Array[Byte] = result.getValue(familyBytes, Bytes.toBytes(x._1))
            (x._1, if (valueBytes == null || valueBytes.isEmpty) null else convert2Value(x._2, valueBytes))
          }).filter(_._2 != null)
        } else
          throw new IllegalArgumentException("family name can not be null ...")
    }
  }


  /**
    * 删除表中的指定行
    *
    * @param tableName 表名
    * @param rowKey    rowkey
    *                  TODO: 批量删除
    */
  @throws[IOException]
  def delete(tableName: String, rowKey: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val delete = new Delete(Bytes.toBytes(rowKey))
    table.delete(delete)
  }

  /**
    * 删除表中的指定行
    *
    * @param tableName 表名
    * @param rowKey    rowkey
    *                  TODO: 批量删除
    */
  @throws[IOException]
  def deleteV2(tableName: String, rowKey: Any): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val delete = new Delete(convert2Bytes(rowKey))
    table.delete(delete)
  }

  def close(): Unit = {
    admin.close()
    connection.close()
  }
}
