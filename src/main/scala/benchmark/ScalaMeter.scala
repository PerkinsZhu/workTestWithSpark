package benchmark

/**
  * Created by PerkinsZhu on 2018/6/22 19:34
  **/

import org.scalameter.api
import org.scalameter.api._

object RangeBenchmark extends Bench.LocalTime {
  val sizes = Gen.range("size")(500000, 1500000, 500000)

  val ranges = for {
    size <- sizes
  } yield 0 until size

  //  使用ranges测试map
  performance of "Range" in {
    measure method "map" in {
      using(ranges) in {
        r => r.map(_ + 1)
      }
    }
  }

  //  使用ranges测试foreach
  performance of "Range" in {
    measure method "foreach" in {
      using(ranges) in {
        r => r.foreach(_ + 1)
      }
    }
  }
}

object ListBenchmark extends Bench.OfflineRegressionReport {

  test01()

  def test03(): Unit = {
    val range = for {
      data <- Gen.range("x")(0, 1000000, 10000)
    } yield 0 until data toList

    performance of "List" in {
      measure method "testForeach" in {
        using(range)  config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => d.foreach(i => i + 1)
        }
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => d.map(i => i + 1)
        }
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => {
            for (i <- d) {
              i + 1
            }
          }
        }

        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => {
            val size = d.size
            var index = 0
            while (index < size) {
              index += 1
            }
          }
        }

      }
    }
  }

  def test01(): Unit = {
    val range = for {
      data <- Gen.range("x")(0, 1000000, 10000)
    } yield 0 until data toList

    performance of "List" in {
      measure method "testForeach" in {
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => d.foreach(i => i + 1)
        }
      }
      measure method "testMap" in {
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => d.map(i => i + 1)
        }
      }
      measure method "testFor" in {
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => {
            for (i <- d) {
              i + 1
            }
          }
        }
      }
      measure method "testWhile" in {
        using(range) config(
          exec.benchRuns -> 2, // we want 10 benchmark runs
          exec.independentSamples -> 1, // and one JVM instance
          exec.maxWarmupRuns -> 10
        ) in {
          d => {
            val size = d.size
            var index = 0
            while (index < size) {
              index += 1
            }
          }
        }
      }
    }


  }

  def test02(): Unit = {
    val tupledLists = for {
      a <- Gen.range("a")(0, 100, 20)
      b <- Gen.range("b")(100, 200, 20)
    } yield ((0 until a).toList, (100 until b).toList)

    performance of "List" in {
      measure method "zip" in {
        using(tupledLists) config(
          exec.benchRuns -> 10, // we want 10 benchmark runs
          exec.independentSamples -> 1 // and one JVM instance
        ) in {
          case (a, b) => a.zip(b)
        }
      }
    }
  }

  //  可以根据需求对measurer进行切换
  //  override def measurer: Measurer[Double] = new MemoryFootprint
  override def measurer = new api.Measurer.IgnoringGC

  // GZIPJSONSerializationPersistor is default but we want to choose custom path for regression data
  override def persistor: Persistor = new GZIPJSONSerializationPersistor("target/results")
}


object Temp {
  def main(args: Array[String]): Unit = {
    val list = 0 until (5)
    val start = System.nanoTime()
    list map (i => i + 1)
    println(System.nanoTime() - start)

    val start2 = System.nanoTime()
    var i =0
    while(i < list.size ){
      i +=1
    }
    println(System.nanoTime() - start2)
  }
}