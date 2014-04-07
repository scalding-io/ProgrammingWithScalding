import com.twitter.scalding._

// foldLeft example
class foldLeft(args: Args) extends Job(args) {

  // A set of customers. Some of them are paying for service-A
  // some of them for service-B etc
  val customerList = List(
    // Customer 1 is paying for service-A AND service-B
    ("customer1", "service-A", true),
    ("customer1", "service-B", true),
    // Customer 2 is paying for service-A
    ("customer2", "service-A", true),
    ("customer2", "service-B", false),
    // Customer 3 is paying for service-B
    ("customer3", "service-A", false),
    ("customer3", "service-B", true),
    // Customer 4 is not paying
    ("customer4", "service-A", false),
    ("customer4", "service-B", false))

    // To identify paying-customers we can use a foldLeft on the group
    // The foldLeft in Scalding is returning a single result
  val payingCustomer =
    IterableSource[(String, String, Boolean)](customerList, ('customer, 'service, 'is_paying))
    .groupBy('customer) {
      // By default a customer is NOT paying - unless there is a 'true'
      group => group.foldLeft('is_paying -> 'is_paying)(false) {
        (prev: Boolean, current: Boolean) =>
          prev || current
      }
    }
    .debug
    .write(Tsv("foldLeft"))

  /*
   Result is:
     customer1 true
     customer2 true
     customer3 true
     customer4 false
   */
}

