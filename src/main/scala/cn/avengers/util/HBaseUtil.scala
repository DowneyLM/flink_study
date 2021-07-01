package cn.avengers.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 * HBase的工具类
 * 获取Table 如果没有则创建
 * 保存单列数据
 * 查询单列数据
 * 保存多列数据
 * 删除数据
 */
object HBaseUtil {

  //HBase的配置类，不需要指定配置文件名，文件名要求是hbase-site.xml
  private val conf: Configuration = HBaseConfiguration.create()
  private val connection: Connection = ConnectionFactory.createConnection(conf)
  private val admin: Admin = connection.getAdmin

  def init(tableNameStr : String , columnFamily : String)  = {

    val tableName: TableName = TableName.valueOf(tableNameStr)

    //构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    //构造列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)
    hTableDescriptor.addFamily(hColumnDescriptor)

    //如果表不存在则创建表
    if(!admin.tableExists(tableName)){
      admin.createTable(hTableDescriptor)
    }

    connection.getTable(tableName)
  }

  /**
   * 根据rowkey，列名查询数据
   * @param tableNameStr  表名
   * @param rowkey        rowkey
   * @param columnFamily  列族名
   * @param column        列名
   * @return 数据
   */
  def getData(tableNameStr : String, rowkey : String, columnFamily : String , column : String) = {

    val table = init(tableNameStr, columnFamily)
    var tmp = ""

    try{
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get = new Get(bytesRowkey)
      val result = table.get(get)
      val value: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column.toString))

      if(value != null && value.size > 0){
        tmp = Bytes.toString(value)
      }catch {
        case e : Exception => e.printStackTrace()
      }finally {
        table.close()
      }
      tmp
    }

  }


  /**
   * 批量获取列的数据
   * @param tableNameStr 表名
   * @param rowkey rowkey
   * @param columnFamily 列族
   * @param columnList 列的名字列表
   * @return
   */
  def getMapData(tableNameStr : String, rowkey : String, columnFamily : String, columnList : List[String]) = {

    val table = init(tableNameStr, columnFamily)
    var tmp = ""

    try{
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get = new Get(bytesRowkey)
      val result = table.get(get)
      val valueMap = collection.mutable.Map[String, String]()

      columnList.map(
        col => {
          val values = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(col))

          if(values != null && values.size > 0 ){
            col -> Bytes.toString(values)
          }else{
            "" -> ""
          }
        }
      ).filter(_._1 != "").toMap
    }catch {
      case e:Exception => e.printStackTrace()
        Map[String,String]()
    }finally {
      table.close()
    }

  }


  /**
   * 插入/更新一条数据
   * @param tableNameStr 表名
   * @param rowkey rowkey
   * @param columnFamily 列族
   * @param column 列名
   * @param data 数据
   */
  def putData(tableNameStr : String, rowkey : String, columnFamily : String, column : String, data : String) = {

    val table = init(tableNameStr, columnFamily)

    try{
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column.toString),Bytes.toBytes(data.toString))
      table.put(put)
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      table.close()
    }
    print("数据已经存入HBase")
  }


  /**
   * 使用Map封装数据，插入/更新一批数据
   * @param tableNameStr 表名
   * @param rowKey rowkey
   * @param columnFamily 列蔟
   * @param mapData key:列名，value：列值
   */
  def putMapData(tableNameStr: String, rowKey: String, columnFamily: String, mapData: Map[String, Any]) = {
    val table: Table = init(tableNameStr, columnFamily)
    try {
      val put: Put = new Put(Bytes.toBytes(rowKey))
      if (mapData.size > 0) {
        for ((k, v) <- mapData) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v.toString))
        }
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
    println("数据已存入HBase")
  }

  /**
   * 根据rowkey删除一条数据
   *
   * @param tableNameStr 表名
   * @param rowkey rowkey
   */
  def deleteData(tableNameStr:String, rowkey:String, columnFamily:String) = {
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // 构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    // 构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)

    hTableDescriptor.addFamily(hColumnDescriptor)
    // 如果表不存在则创建表
    if (!admin.tableExists(tableName)) {
      admin.createTable(hTableDescriptor)
    }

    val table = connection.getTable(tableName)

    try {
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      table.close()
    }
  }


}
