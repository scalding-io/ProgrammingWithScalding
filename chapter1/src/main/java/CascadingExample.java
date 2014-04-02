import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;

import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import java.util.Properties;

// Remove in HDFS mode
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;

// and use following ones
//import cascading.flow.hadoop.HadoopFlowConnector;  // Instead of LocalFlowConnector
//import cascading.scheme.hadoop.TextDelimited;      // Instead of local.TextDelimited
//import cascading.tap.hadoop.Lfs;                   // Instead of FileTap

public class CascadingExample {

    public static void main( String[] args ) {

        Fields schema = new Fields("productID", "customerID", "quantity");
        Tap in = new FileTap(new TextDelimited(schema, false, "," ), "src/main/resources/products.tsv");
        Tap out = new FileTap(new TextDelimited(true, "\t"), "output" );


        // Elastic Search configuration
//        Properties properties = new Properties();
//        properties.setProperty("es.mapping.id", "productID");
//        properties.setProperty("es.write.operation","create");
//        Tap out = new FileTap(new TextDelimited(), "output" );
//        FlowConnector flow = new LocalFlowConnector(properties);
//        flow.connect(in, out, new Pipe("write-to-ES")).complete();

    }
}
