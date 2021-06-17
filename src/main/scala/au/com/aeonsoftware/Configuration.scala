package au.com.aeonsoftware

object Configuration {
  val TABLE_NAMES = Array("customer","orders","new_order","item","order_line","district","history")
  val topicNames = TABLE_NAMES.map(s => "io.aeon.pg.tpcc.public." + s).mkString(",")
}
