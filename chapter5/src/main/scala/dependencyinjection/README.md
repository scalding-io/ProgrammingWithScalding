Compile examples with:

    chapter5 $ mvn clean install

and execute dependency injection with

    chapter5 $ java -cp target/chapter5-0-jar-with-dependencies.jar 
     com.twitter.scalding.Tool dependencyinjection.ExampleJob --local 
     --input src/main/resources/logs.tsv --output output.txt

or if you with to use 'subcut' with

    chapter5 $ java -cp target/chapter5-0-jar-with-dependencies.jar 
     com.twitter.scalding.Tool dependencyinjection.subcut.ExampleJob --local 
     --input src/main/resources/logs.tsv --output output.txt