Before executing this example, have a working HBase, and

    $ hbase shell

    hbase(main):003:0> create 'spyglass.hbase.test1' , 'data'
    hbase(main):006:0> put 'spyglass.hbase.test1' , 'row1' , 'data:column1' , 'value1'
    hbase(main):007:0> put 'spyglass.hbase.test1' , 'row2' , 'data:column1' , 'value2'
    hbase(main):008:0> put 'spyglass.hbase.test1' , 'row3' , 'data:column1' , 'value3'
    hbase(main):009:0> scan 'spyglass.hbase.test1'