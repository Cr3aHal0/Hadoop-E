package main.java.io.github.cr3ahal0.hadoope.crawl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * PageRank CrawlerPigReducer : modifying format for Pig implementation
 */
public class CrawlerPigReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pagerank = "1.0\t{";

        boolean first = true;

        for (Text value : values) {
            if(!first) pagerank += ",";

            pagerank += "(" + value.toString() + ")";
            first = false;
        }

	pagerank += "}";
        context.write(key, new Text(pagerank));
    }

}
