package main.java.io.github.cr3ahal0.hadoope.crawl;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PageRank CrawMapper
 */
public class CrawlerMapper extends Mapper<LongWritable, Text, Text, Text> {


    class Crawler extends WebCrawler {

        private final Pattern FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg"
                + "|png|mp3|mp3|zip|gz))$");

        @Override
        public boolean shouldVisit(Page referringPage, WebURL url) {
            String href = url.getURL().toLowerCase();
            return !FILTERS.matcher(href).matches()
                    && href.startsWith("http://www.ics.uci.edu/");
        }

        /**
         * This function is called when a page is fetched and ready
         * to be processed by your program.
         */
        @Override
        public void visit(Page page) {
            String url = page.getWebURL().getURL();
            System.out.println("URL: " + url);

            if (page.getParseData() instanceof HtmlParseData) {
                HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                Set<WebURL> links = htmlParseData.getOutgoingUrls();

                Set<String> exist = results.get(url);
                if (exist == null) {
                    exist = new HashSet<String>();
                    results.put(url, exist);
                }

                for(WebURL found : links) {
                    results.get(url).add(found.getURL());
                }
            }
        }
    };

    private static Map<String, Set<String>> results = new HashMap<String, Set<String>>();

    private final static int MAX_DEPTH = 20;

    private final static int MAX_ITER = 500;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //crawl("https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal", 0);

        String crawlStorageFolder = "./test";
        int numberOfCrawlers = 7;

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);
        config.setMaxDepthOfCrawling(MAX_DEPTH);
        config.setMaxPagesToFetch(MAX_ITER);
        /*
         * Instantiate the controller for this crawl.
         */
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = null;
        try {
            controller = new CrawlController(config, pageFetcher, robotstxtServer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*
         * For each crawl, you need to add some seed urls. These are the first
         * URLs that are fetched and then the crawler starts following links
         * which are found in these pages
         */
        controller.addSeed("https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal");

        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.start(Crawler.class, numberOfCrawlers);


        //This is a very inconvenient solution since it requires to wait for an entire function execution before printing to context (WIP)
        //It is a very shitty (but temporary) solution since you have to fullfy HashMaps, Sets... to explore them and print their content
        //TL;DR:this sucks
        Set<String> keys = results.keySet();
        for (String cle : keys) {
            Set<String> pages = results.get(key);
            for (String page : pages) {
                context.write(new Text(cle), new Text(page));
            }
        }
    }


}
