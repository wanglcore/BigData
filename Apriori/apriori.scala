package com.Apriori
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.Sorting.quickSort

object Main {

  val conf = new SparkConf().setAppName("apriori").setMaster("local")
  val sc = new SparkContext(conf)
  def main(args: Array[String])
  {
    val support = 100
    val filePath = "hdfs://127.0.0.1:9000//input/k.txt"
    apriori(filePath, support)
  }

  def apriori(filePath: String, support: Int) =
  {
    val transactions = sc.textFile(filePath)
    val numOfTransactions: Double = transactions.count
    val L_1 = aprioriPass1(transactions, support)
    val freqItems = getL1FreqItems(L_1)
    var L_kM = L_1
    var k: Int = 2
    while(!L_kM.isEmpty())
    {
      val freqCombos = generateCandidatesSet(L_kM, freqItems,k)
      val L_k = generateL_k(transactions, freqCombos, support, k)
      L_kM = L_k
      k += 1
    }
  }
  def aprioriPass1(transactions: org.apache.spark.rdd.RDD[String], support: Int): org.apache.spark.rdd.RDD[(String, Int)] =
  {
    val items = transactions.flatMap(txn => txn.split(" "))
                            .map(item=>(item,1))
                            .reduceByKey(_+_)
                            .filter({case(item,sup)=>sup>support})
    items.saveAsTextFile("/home/wangl/Desktop/result/1")
    return items
  }

  def getL1FreqItems(L1: org.apache.spark.rdd.RDD[(String, Int)]): org.apache.spark.rdd.RDD[String]  =
  {
    val freqItems = L1.map{ case (str, sup) => str }
    return freqItems
  }

  def generateCandidatesSet(L_kM: org.apache.spark.rdd.RDD[(String, Int)], freqItems_L1: org.apache.spark.rdd.RDD[String],k:Int)
  :org.apache.spark.rdd.RDD[String] =
  {
    val itemsets = L_kM.map{ case (str, sup) => str }
      .cartesian(freqItems_L1)
      .map(x=>x.toString.replaceAll("[\\[()\\]]", "").split(",").toSet)
      .filter(tupleSet => tupleSet.size == k)
      .map(strArr => strArr.map(_.toInt)).collect
      .map(setOfInts => setOfInts.mkString(","))
    val freqItemCombosRdd=sc.parallelize(itemsets.toSeq)
    return freqItemCombosRdd
  }

  def generateL_k(transactions: org.apache.spark.rdd.RDD[String], freqCombos: org.apache.spark.rdd.RDD[String], support: Int, k: Int)
  : org.apache.spark.rdd.RDD[(String, Int)]
  =
  {
    val freqCombosSets = freqCombos.collect().toSet
    val L_k = transactions.map(txn => txn.split(" "))
      .map(it=>it.map(_.toInt))
      .map(txnItems => txnItems.combinations(k).toArray)
      .map(arrOfarr => arrOfarr.map(arrOfInt => arrOfInt.toSet.mkString(",")))
      .map(arr => arr.filter(tuple => freqCombosSets.contains(tuple)))
      .flatMap(arrayOfPairs => arrayOfPairs.map(array => (array,1)))
      .reduceByKey(_+_)
      .filter{case (str,sup) => sup >= support}
    if(L_k.isEmpty())
    {
      println("No more frequent itemsets.")
    }
    else
    {
      println("\nFrequent itemsets:")
      L_k.saveAsTextFile("/home/wangl/Desktop/result/"+k)
      L_k.foreach(println)
    }

    return L_k
  }
}