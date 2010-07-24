package me.handol.scala

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import util.Properties
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}

/**
 * Created by IntelliJ IDEA.
 * User: dolhana
 * Date: Jul 23, 2010
 * Time: 4:42:53 PM
 * To change this template use File | Settings | File Templates.
 */

object HBaseSample {

  def main(args: Array[String]) {
    val conf = HBaseConfiguration create

    println ("hbase.zookeeper.quorum" + (Properties envOrNone "hbase_zookeeper_quorum"))

    conf.set(
      "hbase.zookeeper.quorum",
      Properties.envOrElse(
        "hbase_zookeeper_quorum",
        conf.get("hbase.zookeeper.quorum")))

    val admin = new HBaseAdmin(conf)

    // list the tables
    //val listtables=admin.listTables()
    //listtables.foreach(println)

    if (admin.tableExists("mytable"))
      admin.disableTable("mytable")
      admin.deleteTable("mytable")

    val tableDesc = new HTableDescriptor(Bytes.toBytes("mytable"))
    val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("ids"))
    tableDesc.addFamily(idsColumnFamilyDesc)
    admin.createTable(tableDesc)

    // let's insert some data in 'mytable' and get the row

    val table = new HTable(conf, "mytable")

    val theput= new Put(Bytes.toBytes("rowkey1"))

    theput.add(Bytes.toBytes("ids"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
    table.put(theput)

    val theget= new Get(Bytes.toBytes("rowkey1"))
    val result=table.get(theget)
    val value=result.value()
    println(Bytes.toString(value))
  }


}
