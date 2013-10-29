call mvn package exec:java -Dexec.mainClass=HelloWorld
echo Sleep 5 seconds!
call ping 1.3.5.42 -n 1 -w 5000 > nul