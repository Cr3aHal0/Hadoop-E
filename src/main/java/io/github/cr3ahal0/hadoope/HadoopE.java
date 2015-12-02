package main.java.io.github.cr3ahal0.hadoope;

import main.java.io.github.cr3ahal0.hadoope.calc.CalculateMapper;
import main.java.io.github.cr3ahal0.hadoope.calc.CalculateReducer;
import main.java.io.github.cr3ahal0.hadoope.crawl.CrawlerMapper;
import main.java.io.github.cr3ahal0.hadoope.crawl.CrawlerReducer;
import main.java.io.github.cr3ahal0.hadoope.rank.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Created by Maxime on 30/11/2015.
 */
public class HadoopE extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HadoopE(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted = runCrawler("files/in", "files/ranking/iter00");
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int runs = 0; runs < 5; runs++) {
            String inPath = "files/ranking/iter" + nf.format(runs);
            lastResultPath = "files/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankOrdering(lastResultPath, "file/result");

        if (!isCompleted) return 1;
        return 0;
    }

    public boolean runCrawler(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job xmlHakker = Job.getInstance(conf, "crawler");
        xmlHakker.setJarByClass(HadoopE.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlHakker, new Path(inputPath));
        xmlHakker.setInputFormatClass(TextInputFormat.class);
        xmlHakker.setMapperClass(CrawlerMapper.class);
        xmlHakker.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlHakker, new Path(outputPath));
        xmlHakker.setOutputFormatClass(TextOutputFormat.class);

        xmlHakker.setOutputKeyClass(Text.class);
        xmlHakker.setOutputValueClass(Text.class);
        xmlHakker.setReducerClass(CrawlerReducer.class);

        return xmlHakker.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "calculator");
        rankCalculator.setJarByClass(HadoopE.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(CalculateMapper.class);
        rankCalculator.setReducerClass(CalculateReducer.class);

        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "ranking");
        rankOrdering.setJarByClass(HadoopE.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }

}
