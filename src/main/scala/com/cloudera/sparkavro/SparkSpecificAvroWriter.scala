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

import com.esotericsoftware.kryo.Kryo

import org.apache.avro.Schema.Parser
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}

object SparkSpecificAvroWriter {
  class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[User])
    }
  }

  def main(args: Array[String]) {
    val outPath = args(0)

    val sparkConf = new SparkConf().setAppName("Spark Avro")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.kryo.registrator", classOf[MyRegistrator].getName)
    val sc = new SparkContext(sparkConf)

    val user1 = new User("Alyssa", 256, null)
    val user2 = new User("Ben", 7, "red")

    val records = sc.parallelize(Array(user1, user2))
    val withValues = records.map((x) => (new AvroKey(x), NullWritable.get))

    val conf = new Job()
    FileOutputFormat.setOutputPath(conf, new Path(outPath))
    val schema = new Parser().parse(getClass.getClassLoader.getResourceAsStream("user.avsc"))
    AvroJob.setOutputKeySchema(conf, schema)
    conf.setOutputFormatClass(classOf[AvroKeyOutputFormat[User]])
    withValues.saveAsNewAPIHadoopDataset(conf.getConfiguration)
  }
}