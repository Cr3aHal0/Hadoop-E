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
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Maxime on 08/12/2015.
 */
public class Crawler extends WebCrawler {

    private final Pattern FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg"
            + "|png|mp3|mp3|zip|gz))$");

    /**
     * This method receives two parameters. The first parameter is the page
     * in which we have discovered this new url and the second parameter is
     * the new url. You should implement this function to specify whether
     * the given url should be crawled or not (based on your crawling logic).
     * In this example, we are instructing the crawler to ignore urls that
     * have css, js, git, ... extensions and to only accept urls that start
     * with "http://www.ics.uci.edu/". In this case, we didn't need the
     * referringPage parameter to make the decision.
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILTERS.matcher(href).matches();
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

            List<WebURL> pages = new ArrayList<WebURL>();
            pages.addAll(links);
            String output = page.getWebURL().getURL() + "\t1.0\t";
            for (WebURL link : pages) {
                output += link.getURL() +",";
            }

            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("files/in/data", true)))) {
                out.println(output);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String url = (args.length > 0) ? args[0] : "http://reddit.com/r/leagueoflegends/,http://www.reddit.com/r/GlobalOffensive/";
        int maxIter = (args.length > 1) ? Integer.valueOf(args[1]) : 5000;
        int maxDepth = (args.length > 2) ? Integer.valueOf(args[2]) : 10;

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

}
