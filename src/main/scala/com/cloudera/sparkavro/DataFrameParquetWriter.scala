/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.sparkavro

import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

object DataFrameParquetWriter {

  case class User(name: String, favorite_number: Int, favorite_color: String)

  def main(args: Array[String]) {
    val outPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark DataFrame Parquet")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    /** import needed to convert a sequence to a DataFrame */
    import sqlContext.implicits._

    /** Sets the compression codec use when writing Parquet files.
      * Acceptable values include: uncompressed, snappy, gzip, lzo. */
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val df = Seq(User("Jeff Mahoney", 256, null), User("Derek", 7, "red")).toDF()

    df.write.parquet(outPath)
  }
}
