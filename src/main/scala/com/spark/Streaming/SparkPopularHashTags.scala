package com.spark.Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
  * A Spark Streaming application that receives tweets on certain
  * keywords from twitter data source and find the popular hashtags
  *
  * Arguments: <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <keyword_1> ... <keyword_n>
  * <consumerKey>				- Twitter consumer key
  * <consumerSecret>  	- Twitter consumer secret
  * <accessToken>				- Twitter access token
  * <accessTokenSecret>	- Twitter access token secret
  * <keyword_1>					- The keyword to filter tweets
  * <keyword_n>					- Any number of keywords to filter tweets
  *
  */

object SparkPopularHashTags {

  val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Set the Spark StreamingContext to create a DStream for every 2 seconds
    val ssc = new StreamingContext(sc, Seconds(2))

    // Pass the filter keywords as arguments to stream
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Split the stream on space and extract hash-tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hash-tags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hash-tags over the previous 10 sec window
//    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//      .map { case (topic, count) => (count, topic) }
//      .transform(_.sortByKey(false))


    // Print popular hash-tags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
//    topCounts10.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}   