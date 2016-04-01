package com.neandertal.pupdownloader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Uses JSoup to parse HTML.
 * 
 * @author neandertal
 *
 */

public class WebCrawler
{
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";

    private static final String KEY_SITE = "site";
    private static final String KEY_KEYWORD = "keyword";
    private static final String KEY_VISITED_URLS_FILE = "visited_urls_file";
    private static final String KEY_MATCHING_URLS_FILE = "matching_urls_file";
    private static final String KEY_TOVISIT_URLS_FILE = "tovisit_urls_file";
    private static final String KEY_LOG_FILE = "log_file";

    private static final String DEFAULT_VISITED_URLS_FILE = "webcrawler/visited_urls.txt";
    private static final String DEFAULT_MATCHING_URLS_FILE = "webcrawler/matching_urls.txt";
    private static final String DEFAULT_TOVISIT_URLS_FILE = "webcrawler/tovisit_urls.txt";
    private static final String DEFAULT_LOG_FILE = "webcrawler/log.txt";
    private static final String UTF8 = "UTF-8";

    private Map<String, String> parameters;
    private URL siteURL;

    private HashSet<String> visitedURLs = new HashSet<String>();
    private HashSet<String> toVisitURLs = new HashSet<String>();
    private HashSet<String> matchingURLs = new HashSet<String>();

    private int pushLimit = 100;
    private int[] lastSaveVisited = new int[1];
    private int[] lastSaveMatching = new int[1];

    private BufferedWriter logFile;
    private BufferedWriter visitedFile;
    private BufferedWriter matchingFile;

    private static AtomicBoolean toQuit = new AtomicBoolean(false);

    public static void main(String[] args) throws IOException
    {
        Thread listen = new Thread(new KeyListenerProcess());
        listen.start();

        WebCrawler wc = new WebCrawler();
        long start = System.currentTimeMillis();
        try
        {
            wc.init(args);
            wc.work();
        }
        catch (Exception e)
        {
            wc.log(e);
        }
        finally
        {
            long time = System.currentTimeMillis() - start;
            long hours = TimeUnit.MILLISECONDS.toHours(time);
            long minutes = TimeUnit.MILLISECONDS.toMinutes(time)
                    - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(time));
            long seconds = TimeUnit.MILLISECONDS.toSeconds(time)
                    - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time));
            wc.log("Elapsed time: {0} h, {1} m, {2} s", hours, minutes, seconds);

            wc.finish();
        }
    }

    private void init(String[] args) throws Exception
    {
        HttpURLConnection.setFollowRedirects(false);

        readParameters(args);

        logFile = createWriter(parameters.get(KEY_LOG_FILE));

        readFileToList(parameters.get(KEY_MATCHING_URLS_FILE), matchingURLs);
        readFileToList(parameters.get(KEY_VISITED_URLS_FILE), visitedURLs);
        readFileToList(parameters.get(KEY_TOVISIT_URLS_FILE), toVisitURLs);

        visitedFile = createWriter(parameters.get(KEY_VISITED_URLS_FILE));
        matchingFile = createWriter(parameters.get(KEY_MATCHING_URLS_FILE));
    }

    private BufferedWriter createWriter(String fileName) throws Exception
    {
        File file = new File(fileName);
        
        File parent = file.getParentFile();
        if (parent != null)
        {
            parent.mkdirs();
        }
        
        return new BufferedWriter(new PrintWriter(file, UTF8));
    }
    
    private void readParameters(String[] args) throws MalformedURLException
    {
        parameters = new HashMap<String, String>();
        parameters.put(KEY_LOG_FILE, DEFAULT_LOG_FILE);
        parameters.put(KEY_MATCHING_URLS_FILE, DEFAULT_MATCHING_URLS_FILE);
        parameters.put(KEY_VISITED_URLS_FILE, DEFAULT_VISITED_URLS_FILE);
        parameters.put(KEY_TOVISIT_URLS_FILE, DEFAULT_TOVISIT_URLS_FILE);

        for (String arg : args)
        {
            String[] pair = arg.split("=");
            if (pair.length != 2) { throw new IllegalArgumentException("Unknown parameter: " + arg); }

            String key = pair[0];
            String value = pair[1];

            if (KEY_SITE.equals(key))
            {
                parameters.put(KEY_SITE, value);
            }
            else if (KEY_KEYWORD.equals(key))
            {
                parameters.put(KEY_KEYWORD, value);
            }
            else if (KEY_LOG_FILE.equals(key))
            {
                parameters.put(KEY_LOG_FILE, value);
            }
            else if (KEY_MATCHING_URLS_FILE.equals(key))
            {
                parameters.put(KEY_MATCHING_URLS_FILE, value);
            }
            else if (KEY_TOVISIT_URLS_FILE.equals(key))
            {
                parameters.put(KEY_TOVISIT_URLS_FILE, value);
            }
            else if (KEY_VISITED_URLS_FILE.equals(key))
            {
                parameters.put(KEY_VISITED_URLS_FILE, value);
            }
        }

        String siteStr = parameters.get(KEY_SITE);
        if (siteStr == null || siteStr.isEmpty()) { throw new IllegalArgumentException("Missing parameter: site"); }
        siteURL = new URL(siteStr);

        String keyword = parameters.get(KEY_KEYWORD);
        if (keyword == null || keyword.isEmpty()) { throw new IllegalArgumentException("Missing parameter: keyword"); }
    }

    private void readFileToList(String fileName, Collection<String> list)
    {
        log("Reading file: {0}", fileName);
        BufferedReader bufferedReader = null;
        try
        {
            FileReader fileReader = new FileReader(fileName);
            bufferedReader = new BufferedReader(fileReader);
            String line = "";
            while ((line = bufferedReader.readLine()) != null)
            {
                list.add(line);
                log("Loaded URL: {0}", line);
            }
        }
        catch (Exception e)
        {
            log(e);
        }
        finally
        {
            try
            {
                if (bufferedReader != null)
                {
                    bufferedReader.close();
                    log("Closing file: {0}", fileName);
                }
            }
            catch (IOException e)
            {
                log(e);
            }
        }
    }

    private void closeFile(BufferedWriter writer, String fileName)
    {
        if (writer == null) return;
        
        try
        {
            writer.flush();
        }
        catch (Exception e)
        {
            log(e);
        }
        finally
        {
            try
            {
                writer.close();
                log("Closed file: {0}", fileName);
            }
            catch (Exception e)
            {
                log(e);
            }
        }
    }

    private void finish()
    {
        closeFile(visitedFile, parameters.get(KEY_VISITED_URLS_FILE));
        closeFile(matchingFile, parameters.get(KEY_MATCHING_URLS_FILE));
        try
        {
            if (logFile != null)
            {
                logFile.flush();
                logFile.close();
                logFile = null;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();

            try
            {
                if (logFile != null)
                {
                    logFile.close();
                }
            }
            catch (IOException t)
            {
                // empty catch block
            }
        }
    }

    private void addVisitedURL(String url)
    {
        addURL(url, visitedURLs, visitedFile, lastSaveVisited);
    }

    private void addMatchingURL(String url)
    {
        addURL(url, matchingURLs, matchingFile, lastSaveMatching);
    }

    private void addToVisitUrl(String url)
    {
        log("Adding to visit URL: {0}", url);
        toVisitURLs.add(url);
        BufferedWriter toVisitFile = null;
        try
        {
            toVisitFile = new BufferedWriter(new FileWriter(parameters.get(KEY_TOVISIT_URLS_FILE), true));
            toVisitFile.write(url);
            toVisitFile.newLine();
            toVisitFile.flush();
        }
        catch (IOException e)
        {
            log(e);
        }
        finally
        {
            try
            {
                if (toVisitFile != null)
                {
                    toVisitFile.close();
                }
            }
            catch (IOException t)
            {
            }
        }
    }

    private String readToVisitURL()
    {
        log("Read URL to visit.");
        if (toVisitURLs.isEmpty()) { return null; }
        
        Iterator<String> iter = toVisitURLs.iterator();
        String url = iter.next();
        iter.remove();
        
        BufferedWriter toVisitFile = null;
        try
        {
            toVisitFile = new BufferedWriter(new PrintWriter(parameters.get(KEY_TOVISIT_URLS_FILE), UTF8));
            while (iter.hasNext())
            {
                toVisitFile.write(iter.next());
                toVisitFile.newLine();
            }
            toVisitFile.flush();
        }
        catch (IOException e)
        {
            log(e);
        }
        finally
        {
            if (toVisitFile != null)
            {
                try
                {
                    toVisitFile.close();
                }
                catch (IOException e)
                {
                }
            }
        }
        
        return url;
    }

    private void addURL(String url, Collection<String> collection, BufferedWriter writer, int[] lastSave)
    {
        collection.add(url);
        
        try
        {
            writer.write(url);
            writer.newLine();
            if (collection.size() - lastSave[0] > pushLimit)
            {
                writer.flush();
                lastSave[0] = collection.size();
            }
        }
        catch (Exception e)
        {
            log(e);
        }
    }

    private void work()
    {
        if (toVisitURLs.isEmpty())
        {
            toVisitURLs.add(siteURL.toString());
            log("Add start URL: {0}", siteURL.toString());
        }
        
        while (!toVisitURLs.isEmpty())
        {
            if (toQuit.get())
            {
                log("QUIT requested by user.", new Object[0]);
                return;
            }
            
            processURL();
        }
        
        log("Finished.", new Object[0]);
    }

    private void log(String msg, Object... args)
    {
        String message = MessageFormat.format(msg, args);
        System.out.println(message);
        if (logFile != null)
        {
            try
            {
                logFile.write(message);
                logFile.newLine();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void log(Throwable t)
    {
        String message = MessageFormat.format("Error: {0} - {1}", t.getClass().getName(), t.getMessage());
        System.out.println(message);
        if (logFile != null)
        {
            try
            {
                logFile.write(message);
                logFile.newLine();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void processURL()
    {
        String url = readToVisitURL();
        if (url == null)
        {
            log("No URL to visit.", new Object[0]);
            return;
        }
        
        addVisitedURL(url);
        
        log("Loading page at URL: {0}", url);
        try
        {
            if (!checkValid(url))
            {
                return;
            }
            
            Connection connection = Jsoup.connect(url).userAgent(USER_AGENT);
            Document doc = connection.get();
            
            String keyword = parameters.get(KEY_KEYWORD);
            if (doc.toString().contains(keyword))
            {
                log("Found matching URL: {0} for keyword: {1}", url, keyword);
                
                try
                {
                    String imageURL = url.replace("displayImage.php?folder=", "");
                    if (!imageURL.endsWith("/"))
                    {
                        imageURL = String.valueOf(imageURL) + "/";
                    }
                    
                    String suffix = "TileGroup0/0-0-0.jpg";
                    imageURL = String.valueOf(imageURL) + suffix;
                    log("Check URL image: {0}", url);
                    
                    HttpURLConnection con = (HttpURLConnection) new URL(imageURL).openConnection();
                    con.setRequestMethod("HEAD");
                    if (con.getResponseCode() == 200)
                    {
                        log("Add matching URL: {0}", url);
                        addMatchingURL(url);
                    }
                }
                catch (Exception e)
                {
                    log(e);
                }
            }
            
            Elements links = doc.select("a[href]");
            for (Element link : links)
            {
                String linkURL = link.absUrl("href");
                if (!linkURL.contains(siteURL.getHost()))
                {
                    log("Skipping external URL: {0}", linkURL);
                    continue;
                }
                
                int index = linkURL.indexOf('#');
                if (index > 0)
                {
                    linkURL = linkURL.substring(0, index);
                }
                
                try
                {
                    String linkHost = new URL(linkURL).getHost();
                    linkURL = linkURL.replace(linkHost, siteURL.getHost());
                }
                catch (Exception e)
                {
                    log("Not a valid URL: {0}", linkURL);
                    continue;
                }
                
                if (toVisitURLs.contains(linkURL) || visitedURLs.contains(linkURL))
                {
                    continue;
                }
                
                addToVisitUrl(linkURL);
            }
        }
        catch (IOException e)
        {
            log(e);
        }
    }

    private boolean checkValid(String URLName)
    {
        try
        {
            HttpURLConnection con = (HttpURLConnection) new URL(URLName).openConnection();
            con.setRequestMethod("HEAD");
            
            if (con.getResponseCode() != HttpURLConnection.HTTP_OK)
            {
                log("Page does not exist.");
                return false;
            }
            
            String contentType = con.getHeaderField("Content-Type");
            if (contentType == null)
            {
                log("Page unknown format.");
                return false;
            }
            
            if (!"application/xml".equals(contentType) && !"application/xhtml+xml".equals(contentType)
                  && !contentType.startsWith("text/"))
            {
                log("Page unsupported format: {0}", contentType);
                return false;
            }
            
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }
    }
    
    private static class KeyListenerProcess implements Runnable
    {
        private KeyListenerProcess()
        {
        }

        @Override
        public void run()
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            int c = -1;
            do
            {
                try
                {
                    c = in.read();
                    continue;
                }
                catch (Exception e)
                {
                    // empty catch block
                }
            }
            while ('Q' != c && 'q' != c);
            toQuit.set(true);
        }
    }

}