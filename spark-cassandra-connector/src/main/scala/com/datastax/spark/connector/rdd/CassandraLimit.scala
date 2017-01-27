package com.datastax.spark.connector.rdd

sealed trait CassandraLimit

case class CassandraPartitionLimit(perPartitionLimit: Long) extends CassandraLimit {
  require(perPartitionLimit > 0, s"$perPartitionLimit <= 0. Per Partition Limits must be greater than 0")
}
case class SparkPartitionLimit(limit: Long) extends CassandraLimit {
  require(limit > 0, s"$limit <= 0. Limits must be greater than 0")
}

object CassandraLimit {

  def limitToClause
  (limit: Option[CassandraLimit]): String = limit match {
    case Some(SparkPartitionLimit(limit)) => s"LIMIT $limit"
    case Some(CassandraPartitionLimit(limit)) => s"PER PARTITION LIMIT $limit"
    case None => ""
  }

  def limitForIterator(limit: Option[CassandraLimit]): Option[Long] = limit.collect {
    case SparkPartitionLimit(limit) => limit
  }
}

