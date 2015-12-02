package main.java.io.github.cr3ahal0.hadoope.crawl;

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

    private final static int MAX_DEPTH = 20;

    private final static int MAX_ITER = 500;

    private int nbRun = 0;

    private URL baseUrl;

    private static Map<String, Set<String>> results = new HashMap<String, Set<String>>();

    private Set<String> explored = new HashSet<String>();

    private boolean end = false;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        crawl("https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal", 0);

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

    /**
     * Crawl url until max. depth reached
     * @param link the web address we want to explore
     * @param depth the current depth
     */
    public void crawl(String link, int depth) {

        //Do not continue if the limit of iterations has been crossed
        if (end) {
            return;
        }

        //Do not continue if we are too deep
        if (depth > MAX_DEPTH) {
            return;
        }

        //If this page has already been explored, we skipt it
        if (explored.contains(link)) {
            return;
        }

        explored.add(link);

        try {

            Set<String> found = new HashSet<String>();


            /**
             * TODO : get HTML string content from url
             */

            URL url = new URL(link);
            if (baseUrl == null) {
                baseUrl = url;
            }


            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");

            BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));

            String html;

            nbRun++;
            if (nbRun > MAX_ITER) {
                System.out.println("Max iter. encountered");
                end = true;
                return;
            }

            //We consider crawling a page when she's available only
            System.out.println("Crawling " + link + " ...");


            while ((html = br.readLine()) != null) {

                Pattern p = Pattern.compile("href=\"(.*?)\"");
                Matcher m = p.matcher(html);

                while (m.find()) {
                    String got = m.group(1);
                    if (got.startsWith("#") ||
                        got.startsWith(":") ||
                        got.startsWith("/")) {
                        continue;
                    }
                    found.add(got);
                }

                for (String aUri : found) {

                    Set<String> actual = results.get(aUri);
                    if (actual == null) {
                        actual = new HashSet<String>();
                    }

                    actual.add(link);
                    results.put(aUri, actual);
                    crawl(aUri, depth+1);
                }

            }

            br.close();

        } catch (MalformedURLException e) {
            System.out.println("Strange url : "+ link);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Unable to fetch : "+ link);
            e.printStackTrace();
        }

    }

    /**
     * Print a notice about the result
     */
    public void notice() {
        Set<String> keys = results.keySet();
        for (String key : keys) {
            Set<String> pages = results.get(key);
            System.out.println(pages.size() + " pages linking to " + key);
        }
    }

}
