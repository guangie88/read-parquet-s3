/*
Copyright (c) 2018 Chen Weiguang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object Main extends App {
  val conf = ConfigFactory.load()

  // required configuration values
  val awsAccessKey = conf.getString("main.awsAccessKey")
  val awsSecretKey = conf.getString("main.awsSecretKey")
  val s3ParquetPath = conf.getString("main.s3ParquetPath")
  val showCount = conf.getInt("main.showCount")

  val ss = SparkSession.builder
    .master("local")
    .appName("read-parquet-s3")
    .getOrCreate()

  // set up Hadoop configuration
  val sc = ss.sparkContext
  val hadoopConfig = sc.hadoopConfiguration
  hadoopConfig.set("fs.s3a.access.key", awsAccessKey)
  hadoopConfig.set("fs.s3a.secret.key", awsSecretKey)

  // read the parquet from S3
  val dataset = ss.read.parquet(s3ParquetPath)
  dataset.show(showCount)
}
