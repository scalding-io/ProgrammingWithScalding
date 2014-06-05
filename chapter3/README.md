This chapter contains lots of reference examples.

So instead of providing multiple input files, we use this trick

  val kidsList = List(
    ("john", "orange,apple"),
    ("liza", "banana,mango"),
    ("nina", "orange"))

  val pipe =
    IterableSource[(String, String)](kidsList, ('kid, 'fruits))
     .read

in order to introduce mock data to the jobs.

Also in package **avro** you can find examples of writing and reading Avro and snappy compressed Avro files
to test use:

  $ ./writeAvro.sh
  $ ./readAvro.sh
