package main.java.io.github.cr3ahal0.hadoope.calc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * PAgeRank CalculateMapper
 */
public class CalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        //Locate where is the first separator between the initial link and the initial rank
        int pageTabIndex = value.find("\t");

        //Then locate the separator between the rank and all the linked links
        int rankTabIndex = value.find(":", pageTabIndex+1);

        // if there is no link, just skip it
        if(rankTabIndex < 0) {
            return;
        }

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageAndRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);

        String linksString = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength() - (rankTabIndex+1));

        String[] links = linksString.split(",");

        //We need to store the total amount of links for this page to etablish the ranking later
        int totalLinks = links.length;

        for (String link : links){
            Text pageRankTotalLinks = new Text(pageAndRank + totalLinks);
            context.write(new Text(link), pageRankTotalLinks);
        }

        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|" + linksString));
    }

}
