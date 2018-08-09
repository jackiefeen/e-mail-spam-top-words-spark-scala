// spamTopWord.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object spamTopWord {

  /**
  function that returns the single unique words from the ingested mail files
  and their probability to occur in an email and the number of files
  **/

  def probaWordDir(sc:SparkContext)(filesDir:String):(RDD[(String, Double)], Long) = {

  //(3a) This function reads all the text files within a directory named filesDir
  val mails = sc.wholeTextFiles(filesDir)

  //(3b) counts the number of files and stores it in a variable nbFiles
  val nbFiles: Long = mails.count()

  //(3c) splits each text file into a set of unique words (per definition, values in a set are unique)
  //val words = mails.map(a => (a._1, a._2.split(" ").toSet))

  //(3d) removes non informative words from the set of unique words
  val toFilter = Set(".", ":", ",", " ", "/", """\""", "-", "'", "(", ")","@")
  val words = mails.map(a => (a._1, a._2.split(" ").toSet.filter(p => !toFilter.exists(e => p.contains(e)))))

  //(3e) counts number of files in which a word occurs; map stucture: word => number of occurences
  val wordDirOccurency = words.flatMap(a => a._2.map(b => (b, 1))).reduceByKey(_+_)

  //(3f) computes the probabiliy of occurences of a word; map structure: word => probability of occurrences
  val probaWord = wordDirOccurency.map(a => (a._1, a._2.toDouble / nbFiles))

  // returns the probability and the number of files
    (probaWord, nbFiles)
  }


  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {

    val probaWJoined = probaW.leftOuterJoin(probaWC)
    val classOcurrency = probaWJoined.map(x => (x._1, (x._2._1, x._2._2.getOrElse(probaDefault))))
    classOcurrency.map(x => (x._1, x._2._2 * (math.log(x._2._2 / (x._2._1 * probaC)) / math.log(2.0))))

  }

  def main(args: Array[String]) {
  val sc = new SparkContext()
  // this gives back an RDD where the first element equals to the word + probability. Also it gives back the total number of files

  // computes the probability of a single word over all mails
  val probaW_nbFiles = probaWordDir(sc)("hdfs://sandbox.hortonworks.com:8020/tmp/ling-spam/*/*.txt")
  val probaW = probaW_nbFiles._1

  //(5a) computes the the couples (probaWordHam, nbFilesHam) for the directory “ham” and (probaWordSpam, nbFilesSpam) for the directory “spam”.
  val probaSpam_nbFiles = probaWordDir(sc)("hdfs://sandbox.hortonworks.com:8020/tmp/ling-spam/spam/*.txt")
  val probaHam_nbFiles = probaWordDir(sc)("hdfs://sandbox.hortonworks.com:8020/tmp/ling-spam/ham/*.txt")

  //(5b)computes the probability P(occurs,class) for each word
  val probaWC = (probaHam_nbFiles._1, probaSpam_nbFiles._1, probaHam_nbFiles._1.map(x => (x._1, 1 - x._2)), probaSpam_nbFiles._1.map(x => (x._1, 1 - x._2)))

  //(5c):
  // computes the probabilities that a word is part of a certain class
  val probaHam = probaHam_nbFiles._2.toDouble/probaW_nbFiles._2
  val probaSpam = probaSpam_nbFiles._2.toDouble/probaW_nbFiles._2

  // computes the MutualInformationFactor for each possible value of (occurs,class)
  val MutualInfoFac_TrueHam = computeMutualInformationFactor(probaWC._1, probaW, probaHam, 0.2/probaW_nbFiles._2)
  val MutualInfoFac_TrueSpam = computeMutualInformationFactor(probaWC._2, probaW, probaSpam, 0.2/probaW_nbFiles._2)
  val MutualInfoFac_FalseHam = computeMutualInformationFactor(probaWC._3, probaW, probaHam, 0.2/probaW_nbFiles._2)
  val MutualInfoFac_FalseSpam = computeMutualInformationFactor(probaWC._4, probaW, probaSpam, 0.2/probaW_nbFiles._2)

  //(5d)
  // combines all the MutualInformation factors for the possible values of (occurs,class) and reduces by the key, i.e. the words
  val MutualInformation = MutualInfoFac_TrueHam.union(MutualInfoFac_TrueSpam).union(MutualInfoFac_FalseHam).union(MutualInfoFac_FalseSpam).reduceByKey((x,y)=>x+y)
  // prints out on the screen the 20 top words maximizing the MI factor in descending order
  MutualInformation.takeOrdered(20)(Ordering.by{ - _._2}).foreach{println}
  //stores the TopWords into a text file on HDFS
  val TopWords = MutualInformation.takeOrdered(20)(Ordering.by{ - _._2})
  sc.parallelize(TopWords).repartition(1).saveAsTextFile("hdfs://sandbox.hortonworks.com:8020/tmp/NEEF/topWords.txt")
  }
}
