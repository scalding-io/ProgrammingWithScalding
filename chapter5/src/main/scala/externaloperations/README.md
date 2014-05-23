Compile examples with:

    chapter5 $ mvn clean install

and execute external operations design pattern with 

    chapter5 $ java -cp target/chapter5-0-jar-with-dependencies.jar 
     com.twitter.scalding.Tool externaloperations.SimpleJob --local 
     --logs src/main/resources/logs.tsv --users src/main/resources/users.tsv --output output.txt
