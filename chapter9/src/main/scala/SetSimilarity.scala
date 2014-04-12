import com.twitter.scalding.{Tsv, IterableSource, Job, Args}

class SetSimilarity(args: Args) extends Job(args) {

  val input = List(
    ("user1", "item1"),
    ("user1", "item2"),
    ("user1", "item3"),
    ("user2", "item1"),
    ("user2", "item2"),
    ("user2", "item10"))

  val users_items =
    IterableSource[(String, String)](input, ('user, 'item))

  // Step 1 - Calculate item-popularity
  val item_popularity = users_items
    .groupBy('item) { _.size('popularity) }
    .write(Tsv("data/output-Jaccard-step1-item-popularity.tsv"))
  /*
   item-popularity holds how many times each item exists in the data-set i.e.

     item1  3
     item2  2
     item3  1
     item10 1
   */

  // Step 2 - Add item popularity to initial data-set
  val users_items_popularity = users_items.joinWithSmaller('item->'item, item_popularity)
    .write(Tsv("data/output-Jaccard-step2-users-items-popularity.tsv"))
  /*
   users-items-popularity is the input, with an additional column 'popularity i.e.

     user1	item1	  2
     user2	item1	  2
     user2	item10  1
     user1	item2   2
     user2	item2	  2
     user1  item3	  1
     user3  item1   1
   */

  // Step 3 - Generate a vector to calculate item-item-statistics
  val CLONE = users_items_popularity
    .rename(('item, 'popularity) -> ('itemB, 'popularityB))
  val item_item_statistics = users_items_popularity
    .joinWithSmaller('user -> 'user, CLONE)
    .filter('item, 'itemB) { x: (String,String) => x._1 < x._2 }
  /*
   Currently the pipe contains the input data + the popularity of the items i.e.

     user1, item1, 2, item2, 2

   */
   // Step 4 - Calculate statistic for pairs of items
    .groupBy('item, 'itemB) { group => group
        .size('pair_popularity)
        .head('popularity)
        .head('popularityB)
    }
    .write(Tsv("data/output-Jaccard-step4-item-item-statistics.tsv"))
  /*
   item-item-statistcs holds: pairs, popularity of pair and the popularity of each itemi.e.

      item1	  item10	1	2	1
      item1	  item2	  2	2	2
      item1	  item3	  1	2	1
      item10	item2	  1	1	2
      item2	  item3	  1	2	1
   */

   // Step 5 - Calculate Jaccard similarity
  val JaccardPopularity = item_item_statistics
    .map(('popularity, 'popularityB, 'pair_popularity)-> 'jaccard) {
      x:(Double, Double, Double) => {
        val item1_popularity = x._1
        val item2_popularity = x._2
        val pair_popularity = x._3

        // return Jaccard similarity score
        pair_popularity / ( item1_popularity + item2_popularity - pair_popularity)
      }
    }
     .debug
  .write(Tsv("data/output-Jaccard-step5-set-similarity.tsv"))



}