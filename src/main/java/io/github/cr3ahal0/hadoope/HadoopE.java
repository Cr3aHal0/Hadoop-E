package main.java.io.github.cr3ahal0.hadoope;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import main.java.io.github.cr3ahal0.hadoope.calc.CalculateMapper;
import main.java.io.github.cr3ahal0.hadoope.calc.CalculateReducer;
import main.java.io.github.cr3ahal0.hadoope.crawl.Crawler;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Maxime on 30/11/2015.
 */
public class HadoopE extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        int choix = -1;
        Scanner sc = new Scanner(System.in);
        while (choix != 0) {
            System.out.println("What to do ?");
            System.out.println("1) Crawl page");
            System.out.println("2) Run PageRank");
            System.out.println("0) Exit");

            String str = sc.nextLine();
            choix = Integer.valueOf(str);

            switch (choix) {
                case 1: {
                    runCrawler();
                }
                break;

                case 2: {
                    System.exit(ToolRunner.run(new Configuration(), new HadoopE(), args));
                }
                break;

                case 0: {
                    System.exit(-1);
                }
                break;

                default:{
                    System.out.println("Option unknown");
                }
                break;
            }
        }
        sc.close();
    }

    private static void runCrawler() throws Exception {

        Scanner sc = new Scanner(System.in);
        System.out.println("URl to crawl ?");
        String url = sc.nextLine();
        url = (url.length() > 0) ? url : "http://reddit.com/r/leagueoflegends/,http://www.reddit.com/r/GlobalOffensive/";

        System.out.println("URl to crawl ?");
        String maxIterS = sc.nextLine();
        int maxIter = (maxIterS.length() > 0 ) ? Integer.valueOf(maxIterS) : 5000;

        System.out.println("URl to crawl ?");
        String maxDepthS = sc.nextLine();
        int maxDepth = (maxDepthS.length() > 0) ? Integer.valueOf(maxDepthS) : 10;

        String crawlStorageFolder = "files/in";
        int numberOfCrawlers = 7;

        String[] urls = url.split(",");
        List<CrawlController> controllers = new ArrayList<CrawlController>();

        for (String anUrl : urls) {

            CrawlConfig config = new CrawlConfig();
            config.setCrawlStorageFolder(crawlStorageFolder);
            config.setMaxDepthOfCrawling(maxDepth);
            config.setMaxPagesToFetch(maxIter);

            /*
             * Instantiate the controller for this crawl.
             */
            PageFetcher pageFetcher = new PageFetcher(config);
            RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
            RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
            CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

            /*
             * For each crawl, you need to add some seed urls. These are the first
             * URLs that are fetched and then the crawler starts following links
             * which are found in these pages
             */
            controller.addSeed(anUrl);

            /*
             * Start the crawl. This is a blocking operation, meaning that your code
             * will reach the line after this only when crawling is finished.
             */
            controller.startNonBlocking(Crawler.class, numberOfCrawlers);
            controllers.add(controller);
        }

        for (CrawlController controller : controllers)   {
            controller.waitUntilFinish();
            System.out.println("controller over");
        }
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
