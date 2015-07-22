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

// This is needed to allow Avro to be used as the data source for the DataFrame
import com.databricks.spark.avro._

object DataFrameAvroReader {

  def main(args: Array[String]) {
    val inPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark DataFrame Avro")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.avroFile(inPath)
    val names = dataFrame.select("name").collect()

    println("num records: " + names.size)
    println("names: " + names.mkString(", "))
  }
}
