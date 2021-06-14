package au.com.aeonsoftware

case class DebeziumEvent(payload: Payload)

case class Payload(op: String, source: Source, before: Person, after: Person)

case class Source(ts_ms: Long, lsn: Long, table: String)

case class Person(id: Long, locationid: Long, name: String)