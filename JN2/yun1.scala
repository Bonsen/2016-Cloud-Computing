package ttt


import org.apache.spark.rdd.RDD
import org.apache.spark.{sql, SparkContext, SparkConf}

/**
 * Created by hadoop on 2/25/16.
 */
object yun1 {
  var biglength = 0L
  var smalllength = 0L

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("mytest").setMaster("spark://Master:7077").setJars(Array("/home/hadoop/spark-assembly-1.4.0-hadoop2.4.0.jar"))
    val sc = new SparkContext(conf)
    val sqlContext = new sql.SQLContext(sc)

    //val savepath = args(4) //repartition(1).saveAsTextFile("")
    //val linshi = args(5) //

    val file1 = sc.textFile(args(0)).cache() //dian ji
    biglength = file1.count() //all
    println("biglength:" + biglength)
    val file2 = sc.textFile(args(1)).cache() //bian ji

    val q1 = file1.map(_.split("\\s")).map { x => (x(0), x(1)) }.groupByKey()
    println("q1:" + q1)
    val q2chu = file2.map(_.split("\\s")).map { x => (x(0), x(1)) }.groupByKey()
    println("q2chu:" + q2chu)
    val q2ru = file2.map(_.split("\\s")).map { x => (x(1), x(0)) }.groupByKey()
    println("q2ru:" + q2ru)


    //zitu zong gong bian,(id->amount)
    val zongtuallbian = file2.map(_.split("\\s")).map { x => (x(0), 1) }.reduceByKey(_ + _)


    val file3 = sc.textFile(args(2)).cache() //zitu dian ji
    smalllength = file3.count() //all of small
    println("smalllength:" + smalllength)
    val file4 = sc.textFile(args(3)).cache() //zitu bian ji

    val q3 = file3.map(_.split("\\s")).map { x => (x(0), x(1)) }.groupByKey()
    println("q3:" + q3)
    val q4chu = file4.map(_.split("\\s")).map { x => (x(0), x(1)) }.groupByKey()
    println("q4chu:" + q4chu)
    val q4ru = file4.map(_.split("\\s")).map { x => (x(1), x(0)) }.groupByKey()
    println("q4ru:" + q4ru)

    //zitu dian gongji lei
    val smallkind = file3.map(_.split("\\s")).map { x => (x(1), x(0)) }.groupByKey().count()
    println("smallkind:" + smallkind)
    val eachamount = file3.map(_.split("\\s")).map { x => (x(1), 1) }.reduceByKey(_ + _) //bu tong lei shu liang
    println("eachamount:" + eachamount)

    //zitu zong gong bian,(id->amount)
    val zituallbian = file4.map(_.split("\\s")).map { x => (x(0), 1) }.reduceByKey(_ + _)

    val liebeiname = eachamount.map(x => x._1)
    //zong tu Cn kaishi
    Tb(-1,-1,q1,eachamount, zongtuallbian, zituallbian,q2chu,q2ru,q4chu,q4ru,q3)
  }

  var j = 0
  var i = 0
  var k = 0
  var ifok1 = 1
  var iffind = 0
  var zitubian = 0
  var zongtubian = 0
  var chu = 0
  var ru = 0
  var pipeiend=0
  val jiru=scala.collection.mutable.ArrayBuffer[String]()
  var jichuru=scala.collection.mutable.Map[String,scala.collection.mutable.ArrayBuffer[String]]()
  val jixuhao=scala.collection.mutable.ArrayBuffer[String]()
  val jineirong=scala.collection.mutable.Map[String,scala.collection.mutable.ArrayBuffer[String]]()
  val jijieguo=scala.collection.mutable.Set[String]()
  var xiabiao=0

  val freq = scala.collection.mutable.Map[String, Int]()
  val ifin = new Array[Int](10000000)
  //chushihua -1

  var az=0

  ////zongtu dian jihuan
  def Tb(now: Int, m: Int, q1: RDD[(String, Iterable[String])], eachamount: RDD[(String, Int)], zongtuallbian: RDD[(String, Int)], zituallbian: RDD[(String, Int)], q2chu: RDD[(String, Iterable[String])], q2ru: RDD[(String, Iterable[String])], q4chu: RDD[(String, Iterable[String])], q4ru: RDD[(String, Iterable[String])], q3: RDD[(String, Iterable[String])]) {
    //now xu yao gai cheng Long
    if (now == biglength) return
    if (m == 0) {
      //huan
      q1.foreach{ a =>
        if(az==now)ifin(j)=a._1.toInt
        az=az+1
      }
      az=0
      j = j + 1
    }
    if (j == smalllength) {
      //da dao zi tu dian shu
      //do
      for (k <- 0 until j) {
        //xuan zhong dian de zhong lei ge shu
        q1.foreach(a =>
          if (a._1 == ifin(j).toString) freq(a._2.toString()) = freq.getOrElse(a._2.toString(), 0) + 1
        )
      }
      eachamount.foreach { a =>
        freq.foreach { b =>
          if (b._1 == a._1) {
            iffind = 1
            if (b._2 != a._2) ifok1 = 0
          }
        }
        if (iffind == 0 || ifok1 == 0) {
          iffind=0
          ifok1=1
          j = j - 1
          return
        }
      }
      //ruo pi pei, dian tong guo
      if (iffind == 1 && ifok1 == 1) {
        iffind=0
        //bian tiao jian
        zituallbian.foreach {
          a =>
            zitubian = zitubian + a._2
        }
        ifin.foreach { a =>
          zongtuallbian.foreach { b =>
            if (a.toString == b._1) {
              zongtubian = zongtubian + b._2
            }
          }
        }
        if (zongtubian >= zitubian) {
          //bian shu liang tong guo
          q3.foreach { a =>
            ifin.foreach { b =>
              q1.foreach { c =>
                if (c._1 == b.toString) {
                  //zhao dao xu hao dui ying lei
                  if (c._2 == a._2) {
                    //zhao dao xuan chu de zong tu zhong dui ying de zi tu lei de dian
                    //pan duan bian bao han
                    //ru bian
                    q2ru.foreach { d =>
                      if (d._1 == c._1) {
                        q4ru.foreach { e =>
                          if (e._1 == c._1) {
                            //yao pan duan q2 bao han q4
                            e._2.foreach { e1 =>
                              d._2.foreach { d1 =>
                                if(d1==e1){
                                  ru=1
                                }
                              }
                              if(ru==0) return
                              else{
                                //ji lu ru
                                jiru +=c._1
                                ru=0
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  //chu bian
                  q2chu.foreach { f =>
                    if (f._1 == c._1) {
                      q4chu.foreach { g =>
                        if (g._1 == c._1) {
                          //yao pan duan q2 bao han q4
                          g._2.foreach { g1 =>
                            f._2.foreach { f1 =>
                              if(f1==g1){
                                chu=1
                              }
                            }
                            if(chu==0) return
                            else{
                              //ji lu chu
                              jiru.foreach{ z =>
                                if(z==c._1){
                                  jichuru(a._1)+=c._1 //wan cheng chu ru qing kuang pi pei
                                  pipeiend=pipeiend+1
                                }
                              }
                              chu=0
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          if(pipeiend!=j){
            jichuru=jichuru.empty////??
            pipeiend=0
            return
          }
          //da yin print
          jichuru.foreach{ a =>
            println(a._1+" ")
            a._2.foreach(b=>print(b+" "))
          }
          //fen pei
          jichuru.foreach{ a =>
            jixuhao+=a._1
          }
          jichuru.foreach{ a =>
            jineirong(a._1)=a._2
          }//fu zhi
          fenpeiz(0,0)
        }
        else {
          j = j - 1
          return
        }
      }
      j = j - 1
      return
    }
    if (i == 1 && ifin(j) != -1) {
      ifin(j) = -1
      j = j - 1
    }
    for (i <- 0 until 2) {
      Tb(now + 1, i, q1, eachamount, zongtuallbian, zituallbian, q2chu, q2ru, q4chu, q4ru, q3)
    }
  }
  def fenpeiz(now:Int,i:Int) {
    if(now==j)return
    //xuan zhong dang qian
    if(jichuru(jixuhao(now)).length==0){
      jijieguo-jichuru(jixuhao(now-1))(i)
      //huan yuan hou mian
      for(k<-now until j){
        jineirong(jixuhao(k)).foreach{ a =>
          if(!jichuru(jixuhao(k)).contains(a)){
            jichuru(jixuhao(k))+a
          }
        }
      }
      return
    }
    jijieguo+jichuru(jixuhao(now))(i)
    if(now==j-1){
      //shuchu output
      jijieguo.foreach(print(_))
      jijieguo-jichuru(jixuhao(now))(i)
      return
    }

    for(k<-now+1 until j){
      jichuru(jixuhao(k)).foreach{ a =>
        if(a!=jichuru(jixuhao(now))(i)){
          xiabiao=xiabiao+1
        }
      }
      jichuru(jixuhao(k)).remove(xiabiao)
      xiabiao=0
    }//qu diao yi xuan
    for (i <- 0 until jichuru(jixuhao(now)).length) {
      fenpeiz(now+1,i)
    }
    jijieguo-jichuru(jixuhao(now))(i)
  }
}

