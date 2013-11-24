import com.twitter.scalding._

// groupBy example
class groupBy(args: Args) extends Job(args) {

  val kidsList = List(
    ("john", 4, "orange,apple"),
    ("liza", 5, "banana,mango"),
    ("nina", 5 ,"orange"),
    ("x", 4, "none"))

  val orderedPipe =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupBy('fruit) { group => group.size('totalFruits).average('age -> 'averageAge) }
      .debug
      .write(Tsv("Output-groupBy"))

  var kidsList2 = List(
    ("john", 4D, "orange,apple"),
    ("liza", 5D, "banana,mango"),
    ("nina", 5D ,"orange"))

  val kidsSchema = List('kid, 'age, 'fruits)
  val commonGroupOperationsPipe =
//    Csv("kids.tsv" ,"\t", kidsSchema).read
  IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupBy('fruit) { group => group
            .min('age -> 'minAge)
            .max('age -> 'maxAge)
            .average('age -> 'averageAge)
            .sum[Int]('age->'totalAge)    // The [Int] is required only in dev versions i.e 0.9.0rc4
            .size('tatalKids)
            .count('age -> 'age4){ x:Int => x == 4 }
            .sizeAveStdev('age -> ('totalAges, 'meanAge, 'stdevAge))
      }
      .debug
      .write(Tsv("Output-commonGroupOperations",writeHeader=true))

  var productList = List(
    ("productA", 11.2, 10),
    ("productB", 22.5, 22))

  //productList.mkString(",")
  val products =
    IterableSource[(String, Double, Int)](productList, ('productId, 'price, 'quantity))
      .groupBy('productId) { group => group.pass }
      .debug
      .write(Tsv("Output-pgroupBy"))

  val mkString =
    IterableSource[(String, Double, Int)](productList, ('productId, 'price, 'quantity))
      .groupAll { group => group.mkString('productId -> 'productIds, " - ") }
      .debug
      .write(Tsv("Output-mkString", writeHeader=true))

  val sortBy =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupBy('fruit) { group => group.sortBy('age) }
      .debug
      .write(Tsv("Output-sortBy", writeHeader=true))

  val take =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupAll { group => group.take(2) }
      .debug
      .write(Tsv("Output-take", writeHeader=true))

  val takeWhile =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupAll { group => group.takeWhile('age) { x:Int => x <= 4  } }
      .debug
      .write(Tsv("Output-takeWhile", writeHeader=true))

  val sortWithTake =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupAll { group => group.sortWithTake(('age,'kid) -> 'newList, 2) {
        ( prev: (Int,String), next:(Int,String)) => prev._1 > next._1
      }}
      .write(Tsv("Output-sortWithTake", writeHeader=true))

  val sortedReverseTake =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits))
      .flatMap('fruits -> 'fruit) { text : String => text.split(",") }
      .discard('fruits)
      .groupAll { group => group.sortedReverseTake[(Int, String)](( 'age, 'kid) -> 'newList, 2) }
      .debug
      .write(Tsv("Output-sortedReverseTake", writeHeader=true))
  //.flatten //show how to flatten lists

}
