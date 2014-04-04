import cascading.tuple.Fields
import com.twitter.scalding._
import TDsl._
import com.twitter.scalding.mathematics.Matrix

/**
 * Finding the most representative words in every canto using TF-IDF
 * term frequency–inverse document frequency
 * Run it using run tutorial.Tutorial2 --k 20 (or any other number of common words)
 */
class BookSimilarity(args : Args) extends Job(args) {

  // Step 1 - Read input and transform into lines : (book - word)
  val inputSchema = List('book, 'text)
  val books = Tsv("data/books.txt", inputSchema)
    .read
    .flatMap('text -> 'word) { text:String =>
       text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+").filter(_.length > 0)
    }
   .project('book,'word)

  // Step 2 - Calculate the term-frequency - i.e. how many times a word appears in a book
  val tf = books.groupBy('book, 'word) { _.size('tf) }
   .project('book,'word, 'tf)

  // Step 3 - Calculate the document-frequency - i.e. in how many books a word appears
  val df = tf.groupBy('word) { _.size('df)}

  // Step 4 - Calculate inverse document-frequency
  // By joing term-frequency with document-frequency
  val number_of_books = 62
  val tfidf = tf
  .joinWithSmaller('word -> 'word, df)
  .map(('df, 'tf) -> 'idf) {
    x:(Int,Int) =>
      x._1 * math.log(number_of_books / x._2)
  }
  .filter('idf) { x:Double => x > 0}
  .project('book, 'word, 'idf)



  // Let's write output to further understand what's happening in our ETL process
  books.write(Tsv("data/output-books.tsv"))
  tf.write(Tsv("data/output-tf.tsv"))
  df.write(Tsv("data/output-df"))
  tfidf.write(Tsv("data/output-tfidf"))



  // Step 5 - Calculate book - similarity
  import Matrix._
  val booksMatrix = tfidf.toMatrix[String,String,Double]('book, 'word,'idf)
  // Normalize the matrix -
  val normedMatrix = booksMatrix.rowL2Normalize.write(Tsv("norm"))
  val similarities = (normedMatrix * normedMatrix.transpose).pipe.toTypedPipe[(String, String, Double)](Fields.ALL)

  similarities.filter(s => s._1 < s._2).groupAll.sortBy(-_._3).values.write(TypedTsv[(String, String, Double)]("data/output-bookSimilarities.tsv"))

}