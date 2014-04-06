import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

/**
 * Discovery of book-similarity using TF-IDF ( Term Frequency / Inverse Document Frequency)
 *
 * Input is 62 books by The Brothers Grimm
 *
 * Algorithm applies 4 ETL steps to calculate - term-frequency, document-frequency, and inverse-term frequency
 * and then uses the Matrix API to calculate similarity on step 5
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

  // Step 4 - Calculate tf-idf -> inverse document-frequency
  // By joining term-frequency with document-frequency
  val number_of_books = 62
  val tfidf = tf
  .joinWithSmaller('word -> 'word, df)
  .map(('tf, 'df) -> 'tfidf) {
    x:(Int,Int) =>
      // term-frequency * inverse-document-frequency
      x._1 * math.log(number_of_books / x._2) // idf = log ( N / df )
  }
  // When a word exists in ALL documents
  // IDF = log ( D / tf ) = log (62/62) = log(1) = 0
  // So the value 0 can be safely ignored
  .filter('tfidf) { x:Double => x > 0}
  .project('book, 'word, 'tfidf)



  // Step 5 - Calculate book - similarity
  import Matrix._
  val booksMatrix = tfidf.toMatrix[String,String,Double]('book, 'word,'tfidf)
  // Normalize the matrix
  val normedMatrix = booksMatrix.rowL2Normalize
  val similarities = (normedMatrix * normedMatrix.transpose)
    // when a matrix is transformed into a pipe it contains three columns of data
    // 'row , 'col , 'val
    .pipe
    // Rename just for the shake of making code more readable :)
    .rename(('row,'col,'val)->('book1,'book2,'similarity))
    // We now have a full-matrix - that means that
    // book1 - book1 - similarity=1.0
    // book1 - book2 - similarity=0.6
    // book2 - book1 - similarity=0.6
    // We don't care that book1 is 100% similar to book1 :p
    // Also the similarity of book1->book2 is always the same to book2->book1
    // So let's filter out all irrelevant/duplicate data
    .filter('book1, 'book2) { x:(String,String) => x._1 < x._2}
    // groupAll - and sort by the most similar books (descending)
    .groupAll { group => group.sortBy('similarity).reverse }
    .write(Tsv("data/output-book-similarities.tsv"))



  // Let's write output to further understand what's happening in our ETL process
  books.write(Tsv("data/output-bs-books.tsv"))
  tf.write(Tsv("data/output-bs-tf.tsv"))
  df.write(Tsv("data/output-bs-df"))
  tfidf.write(Tsv("data/output-bs-tfidf"))
  normedMatrix.write(Tsv("data/output-bs-normed-matrix"))

}