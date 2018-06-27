//s3://comp9313.z5137591/flickr_london.txt
//s3://comp9313.z5137591/out
//0.85

//-- class comp9313.ass4.SetSimJoin


/**
  * z5137591
  * kai fang
  * comp9313 assignment4
  * The program already set "setMaster("local")"
  * if you wish to run in AWS ,please remove it.
  */

package comp9313.ass4

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SetSimJoin {

  def getRidPair(lines:String,threshold:Double,GlobalTokenRanking:Map[String,Int]):Array[(Int, (Long, Array[Int]))]={

    val line = lines.split(" ")
    val recordId= line.head
    val content= line.slice(1,line.length)
    //*************step 1: calculate prefix length
    // determine prefix length of tokenized string s
    // prefixLength = s - s*t + 1 (s is lenght of string, t is threshold)
    val prefixLength = math.ceil(content.length *(1.0- threshold)).toInt + 1

    //*************step 2: sort based on  GlobalTokenRanking
    // we want to know the top-prefix tokens with highest-rank
    // so we find  rank-token from GlobalTokenrank
    // sort based on rank in ascending order(rank,token)
    val OrderTokens = content.map(token=>GlobalTokenRanking(token)).sorted

    //************** step 3: extract prefix
    // take top-prefix token
    val TopPrefix = OrderTokens.take(prefixLength)
    // sorted content ( use rank instead of tokens for further optimation requirement )
    for(token <- TopPrefix) yield (token,(recordId.toLong,OrderTokens))

  }


  def getRecordJoin(recordsList: Iterable[(Long, Array[Int])], threshold: Double):ArrayBuffer[((Long, Long), Double)]={


    val SimOutput = ArrayBuffer[((Long,Long),Double)]()
    val recordListToArr = ArrayBuffer[(Long, Array[Int])]()
    for (arr <- recordsList) recordListToArr += arr
    val length = recordListToArr.size

    val content = recordListToArr.map(x => x._2)
    val recordID = recordListToArr.map(x => x._1)

    // step 1: iterate all possible pair for each token
    for (i<- 0 until length){
      for(j<- i+1 until length){

        val r1String = content{i}
        val r2String = content{j}
        val r1_length = r1String.length
        val r2_length = r2String.length

        // optimation
        // PPjoin -- length filter
        if(r2_length >= Math.ceil(r1_length * threshold).toInt && r1_length>=Math.ceil(r2_length * threshold).toInt){

          // calculate length of interaction set by merge algorithm with two sorted list
          //   calculate similarity (jaccard)
          var JacardSim = 0.0
          var p1=0
          var p2=0
          var interactionLength = 0
          var counter = 0
          var flag = 0
          // optimation
          //PPjoin -- position filter
          var miniOverlap = math.ceil((threshold / (threshold+1))* (r1_length + r2_length)).toInt

          while (flag == 0 && p1 < r1_length && p2 < r2_length){

            if(r1String(p1) == r2String(p2)) {

              if (counter + math.min(r1_length - p1, r2_length - p2) + 1 < miniOverlap) {
                flag = 2
              }
              //find Sim token
              interactionLength += 1
              p1 += 1
              p2 += 1
            }
            else if (r1String(p1)< r2String(p2)){
              p1 += 1
            }else {p2+=1}
            counter+=1
          }
          JacardSim = interactionLength.toDouble / (r1_length + r2_length - interactionLength).toDouble
          if (JacardSim >= threshold)
            SimOutput.append(((recordID{i},recordID{j}),JacardSim))
        }
      }
    }

    SimOutput
  }


  def main(args: Array[String]){

    val inputFile = args(0)
    val outputFile = args(1)
    val threshold = args(2).toDouble

    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile).persist()

    //------------ stage 1: Token Ordering -------------
    val tokenFrequency = input.flatMap(line => line.split(" ").slice(1, line.length)) // 1.join  attribute values
      .map(token => (token,1))
      .reduceByKey(_+_)  // 2. do word count
      .sortBy(x=>x._2) // 3. sort by term frequency
      .map(x=>x._1) // 4. get sorted token order
      .collect()

    // traveling token order list then create a Hashmap to store  (token,ranking)
    var tokenRanking = Map[String,Int]()
    for(i<-tokenFrequency.indices) tokenRanking += tokenFrequency(i)->i
    // cache GlobalTokenFrequency to each machine
    val GlobalTokenRanking = sc.broadcast(tokenRanking)


    //------------stage 2: RID-Pair Generation------------
    /**
      ****** find record ID with similar join attribute *******
      */

    val recordWithPrefix = input.flatMap(line=>getRidPair(line,threshold,GlobalTokenRanking.value))
      .groupByKey()

    //-----------------stage 3: Record join ------------------

    val SimSet = recordWithPrefix.flatMap{ case(_,x)=>getRecordJoin(x,threshold)}.reduceByKey((a,b)=>a)
      .sortBy(s => (s._1,s._2))
      .map(x=> x._1 + "\t" + x._2)

    SimSet.saveAsTextFile(outputFile)

  }

}



