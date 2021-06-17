package au.com.aeonsoftware

import java.sql.Timestamp


case class DebeziumEvent(payload: Payload)
case class Payload(op: String, source: Source, before: String, after: String)
case class Source(ts_ms: Long, lsn: Long, table: String)
case class Customer(c_id: Float,
                    c_d_id: Float,
                    c_w_id: Float,
                    c_first: String,
                    c_last: String,
                    c_credit: String,
                    c_discount: Float,
                    c_balance: Float,
                    c_ytd_payment: Float,
                    c_payment_cnt: Float,
                    c_delivery_cnt: Float)

case class Orders(o_id: Float,
                  o_w_id: Float,
                  o_d_id: Float,
                  o_c_id: Float,
                  o_ol_cnt: Float,
                  o_entry_d: Timestamp)