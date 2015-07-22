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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object DataFrameParquetReader {

  def main(args: Array[String]): Unit = {
    val inPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark DataFrame Parquet")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    /** Turn on Parquet filter pushdown optimization. This feature is turned off by default
      * because of a known bug in Paruet 1.6.0rc3 */
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")

    val dataFrame = sqlContext.read.parquet(inPath)
    val names = dataFrame.select("name").collect()

    println("num records: " + names.size)
    println("names: " + names.mkString(", "))
  }
}
