# Creates a local file called YYYYmmDD and puts it on HDFS folder /dataset/2014/01/xx/YYYYmmDD for Jan 2014
hadoop fs -rm -r /dataset
for i in 0{1..9} {10..31} ; do
    hadoop fs -mkdir -p /dataset/2014/01/$i/
    echo "2014-01-$i" > 201401$i
    hadoop fs -put 201401$i /dataset/2014/01/$i/
done
