call mvn package assembly:single -Dexec.mainClass=WordCountJob

echo This is a happy day. A day to remember > input.txt
java -cp target/chapter2-0-jar-with-dependencies.jar com.twitter.scalding.Tool WordCountJob --local --input input.txt --output output.txt -Xmx1024mjava

echo Sleep 5 seconds!
call ping 1.3.5.42 -n 1 -w 5000 > nul
