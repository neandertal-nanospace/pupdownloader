package com.neandertal.pupdownloader;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.imageio.ImageIO;

import net.coobird.thumbnailator.Thumbnails;

/**
 * Download Zoomify image
 * 
 * @author neandertal
 *
 */
public class ZoomifyDownloader
{
    private static final int BUFFER_SIZE = 4096;
    
    private static final String IMG_FILE_SUFFIX = ".png";
    
    private static final String KEY_LOG_FILE = "log_file";
    private static final String KEY_URLS_FILE = "urls_file";
    private static final String KEY_RESULT_FOLDER = "result_folder";
    
    private static final String DEFAULT_LOG_FILE = "zoomifydownloader/log.txt";
    private static final String DEFAULT_URLS_FILE = "zoomifydownloader/urls.txt";
    private static final String DEFAULT_RESULT_FOLDER = "zoomifydownloader/result";
    
    private static final String RESULT_HTML_FILE = "result.html";
    private static final String UTF8 = "UTF-8";
    
    private Map<String, String> parameters;
    private HashSet<String> urlsToVisit = new HashSet<String>();
    private HashMap<String, Page> hashToPage = new HashMap<String, Page>();
    
    private BufferedWriter logFile;
    private int downloadedImages = 0;
    
    private static AtomicBoolean toQuit = new AtomicBoolean(false);

    private static class KeyListenerProcess implements Runnable
    {
        public void run()
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            int c = -1;
            while (true)
            {
                try
                {
                    c = in.read();
                }
                catch (Exception e)
                {
                }

                if ('q' == c || 'Q' == c)
                {
                    toQuit.set(true);
                    break;
                }
            }
        }
    }

    public static void main(String[] args)
    {
        Thread listen = new Thread(new KeyListenerProcess());
        listen.start();

        ZoomifyDownloader zd = new ZoomifyDownloader();
        long start = System.currentTimeMillis();
        try
        {
            zd.init(args);
            zd.work();
        }
        catch (Exception e)
        {
            zd.log(e);
        }
        finally
        {
            long time = System.currentTimeMillis() - start;
            long hours = TimeUnit.MILLISECONDS.toHours(time);
            long minutes = TimeUnit.MILLISECONDS.toMinutes(time)
                    - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(time));
            long seconds = TimeUnit.MILLISECONDS.toSeconds(time)
                    - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time));
            zd.log("Elapsed time: {0} h, {1} m, {2} s", hours, minutes, seconds);

            zd.finish();
        }
    }

    private void work() throws Exception
    {
        if (urlsToVisit.isEmpty())
        {
            log("Nothing to do.");
            return;
        }

        for (String url : urlsToVisit)
        {
            if (toQuit.get())
            {
                log("QUIT requested by user.");
                return;
            }

            processURL(url);
        }
    }

    private void processURL(String url)
    {
        try
        {
            Page page = createPage(url);
            hashToPage.put(page.hash, page);

            if (!findTileAndZ(page)) { return; }

            if (!loadImage(page)) { return; }

            if (!saveImage(page)) { return; }

            page.succeeded = true;
        }
        catch (Exception e)
        {
            log(e);
        }
    }

    private boolean saveImage(Page page)
    {
        if (page.image == null) { return false; }

        String imageFileName = parameters.get(KEY_RESULT_FOLDER) + File.separator + page.imageFileName;
        String thumbnailFileName = parameters.get(KEY_RESULT_FOLDER) + File.separator + "thumbnail_" + page.imageFileName;
        try
        {
            ImageIO.write(page.image, "png", new File(imageFileName));
            log("Saved image to file: {0}", imageFileName);
            downloadedImages++;

            // try to create thumbnail
            try
            {
                Thumbnails.of(page.image).size(128, 128).toFile(thumbnailFileName);
            }
            catch (Exception e)
            {
            }

            return true;
        }
        catch (IOException e)
        {
            log("Fail to save image to file: {0}", imageFileName);
            log(e);
        }
        finally
        {
            page.image = null;
        }

        return false;
    }

    private BufferedImage downloadImage(String url, boolean logFail)
    {
        try
        {
            Thread.sleep(100);
        }
        catch (InterruptedException e1)
        {
        }

        InputStream in = null;
        ByteArrayOutputStream out = null;
        try
        {
            URL urlURL = new URL(url);
            in = new BufferedInputStream(urlURL.openStream());
            out = new ByteArrayOutputStream();
            byte[] buf = new byte[BUFFER_SIZE];
            int n = 0;

            while (-1 != (n = in.read(buf)))
            {
                out.write(buf, 0, n);
            }

            BufferedImage img = ImageIO.read(new ByteArrayInputStream(out.toByteArray()));
            return img;
        }
        catch (Exception e)
        {
            if (logFail) log(e);
        }
        finally
        {
            if (in != null)
            {
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                }
            }

            if (out != null)
            {
                try
                {
                    out.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        return null;
    }

    public static boolean exists(String URLName)
    {
        try
        {
            HttpURLConnection con = (HttpURLConnection) new URL(URLName).openConnection();
            con.setRequestMethod("HEAD");
            return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }
    }

    private boolean findTileAndZ(Page page)
    {
        String finalURL = MessageFormat.format(page.maskImagePartURL, 0, 0, 0, 0);
        if (!exists(finalURL))
        {
            log("Not a valid zoomify page: {0}", finalURL);
            return false;
        }

        int tile = 0;
        int z = 3;

        boolean toContinue = true;
        boolean tileFailed = false;
        while (toContinue)
        {
            finalURL = MessageFormat.format(page.maskImagePartURL, tile, z, 0, 0);
            boolean exist = exists(finalURL);

            if (!exist && tileFailed)
            {
                break;
            }

            if (exist)
            {
                page.tile = tile;
                page.z = z;

                z++;
                tileFailed = false;
                continue;
            }

            // try bigger tile
            tileFailed = true;
            tile++;
        }

        return true;
    }

    private boolean loadImage(Page page)
    {
        log("Downloading image started...");
        int y = 0;

        BufferedImage finalImage = null;
        while (true)
        {
            BufferedImage imageRow = loadImageForRow(page, y);
            if (imageRow == null)
            {
                // assume end
                if (page.y == -1)
                {
                    page.y = y - 1;
                }
                break;
            }

            finalImage = appendYImages(finalImage, imageRow);
            y++;
        }

        if (finalImage == null)
        {
            log("Failed to get the image for url: {0}", page.url);
        }
        else
        {
            log("Dowloaded image with size {0}x{1} pixels.", String.valueOf(finalImage.getWidth()),
                    String.valueOf(finalImage.getHeight()));
        }

        page.image = finalImage;
        return finalImage != null;
    }

    private BufferedImage loadImageForRow(Page page, int y)
    {
        int x = 0;
        int tile = page.tile;

        BufferedImage rowImage = null;
        while (true)
        {
            String imagePartURL = MessageFormat.format(page.maskImagePartURL, tile, page.z, x, y);
            boolean exist = exists(imagePartURL);

            if (exist)
            {
                BufferedImage imageX = downloadImage(imagePartURL, false);
                rowImage = appendXImages(rowImage, imageX);
                x++;
                page.tile = tile;
                page.x = x;
                page.parts++;
                continue;
            }
            else
            {
                if (page.tile + 1 == tile)
                {
                    // assume end
                    page.x = x - 1;
                    break;
                }
                else
                {
                    tile++;
                }
            }
        }

        return rowImage;
    }

    private BufferedImage appendXImages(BufferedImage imgRow, BufferedImage imgX)
    {
        if (imgRow == null)
            return imgX;

        int width = imgRow.getWidth() + imgX.getWidth();
        int height = Math.max(imgRow.getHeight(), imgX.getHeight());

        BufferedImage finalImg = new BufferedImage(width, height, imgRow.getType());
        Graphics g = finalImg.getGraphics();
        g.drawImage(imgRow, 0, 0, null);
        g.drawImage(imgX, imgRow.getWidth(), 0, null);

        return finalImg;
    }

    private BufferedImage appendYImages(BufferedImage imgAll, BufferedImage imgY)
    {
        if (imgAll == null)
            return imgY;

        int width = Math.max(imgAll.getWidth(), imgY.getWidth());
        int height = imgAll.getHeight() + imgY.getHeight();

        BufferedImage finalImg = new BufferedImage(width, height, imgAll.getType());
        Graphics g = finalImg.getGraphics();
        g.drawImage(imgAll, 0, 0, null);
        g.drawImage(imgY, 0, imgAll.getHeight(), null);

        return finalImg;
    }

    private Page createPage(String url) throws Exception
    {
        Page page = new Page();

        page.url = url;
        page.hash = getHash(url);

        // remove php middleman
        url = url.replace("displayImage.php?folder=", "");
        if (!url.endsWith("/"))
        {
            url += "/";
        }

        String path = "";
        try
        {
            path = (new URL(url)).getPath();
            path = path.replaceAll("\\W+", "_");
        }
        catch (Exception e)
        {
        }
        // page file path
        page.imageFileName = path + page.hash + IMG_FILE_SUFFIX;

        // add suffix
        String suffix = "TileGroup{0}/{1}-{2}-{3}.jpg";
        url += suffix;
        page.maskImagePartURL = url;

        log("Page url: {0}", page.url);
        log("Page hash: {0}", page.hash);
        log("Page maskImagePartURL: {0}", page.maskImagePartURL);
        log("Page imageFileName: {0}", page.imageFileName);
        return page;
    }

    /**
     * Initialize web crawler parameters.
     * 
     * @throws UnsupportedEncodingException
     * @throws FileNotFoundException
     */
    private void init(String[] args) throws Exception
    {
        HttpURLConnection.setFollowRedirects(false);

        readParameters(args);

        logFile = createWriter(parameters.get(KEY_LOG_FILE));

        log("Init parameters: {0}", Arrays.toString(args));
        readFileToList(parameters.get(KEY_URLS_FILE), urlsToVisit);

        log("Prepare result folder...");
        File resFolder = new File(parameters.get(KEY_RESULT_FOLDER));
        if (resFolder.exists())
        {
            deleteFolder(resFolder);
            log("Old result folder cleaned.");
        }
        resFolder.mkdirs();
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
    
    public static void deleteFolder(File folder)
    {
        File[] files = folder.listFiles();
        if (files != null)
        {
            // some JVMs return null for empty dirs
            for (File f : files)
            {
                if (f.isDirectory())
                {
                    deleteFolder(f);
                }
                else
                {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    /** Load file content into list */
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

    /**
     * Read the input parameters
     * 
     * @param args
     * @return
     */
    private void readParameters(String[] args)
    {
        parameters = new HashMap<String, String>();
        parameters.put(KEY_LOG_FILE, DEFAULT_LOG_FILE);
        parameters.put(KEY_URLS_FILE, DEFAULT_URLS_FILE);
        parameters.put(KEY_RESULT_FOLDER, DEFAULT_RESULT_FOLDER);

        for (String arg : args)
        {
            int index = arg.indexOf('=');
            if (index == -1) { throw new IllegalArgumentException("Unknown parameter: " + arg); }

            String key = arg.substring(0, index);
            String value = arg.substring(index + 1, arg.length());

            if (KEY_LOG_FILE.equals(key))
            {
                parameters.put(KEY_LOG_FILE, value);
            }
            else if (KEY_URLS_FILE.equals(key))
            {
                parameters.put(KEY_URLS_FILE, value);
            }
            else if (KEY_RESULT_FOLDER.equals(key))
            {
                parameters.put(KEY_RESULT_FOLDER, value);
            }
        }
    }

    private void writeResults()
    {
        log("Processed  {0} urls, {1} pages, {2} images.", urlsToVisit.size(), hashToPage.size(), downloadedImages);
        log("Write result file...");
        BufferedWriter resultFile = null;
        try
        {
            String resultFileName = parameters.get(KEY_RESULT_FOLDER) + File.separator + RESULT_HTML_FILE;
            resultFile = new BufferedWriter(new PrintWriter(resultFileName, UTF8));
            resultFile.newLine();
            resultFile.write("<html><head><title>Result</title></head><body>");
            resultFile.newLine();
            resultFile.write("<h1>Operation result</h1>");
            resultFile.newLine();
            resultFile.write(MessageFormat.format("<p>URLs: {0}</p>", String.valueOf(urlsToVisit.size())));
            resultFile.newLine();
            resultFile.write(MessageFormat.format("<p>Pages visited: {0}</p>", String.valueOf(hashToPage.size())));
            resultFile.newLine();
            resultFile.write(MessageFormat.format("<p>Downloaded images: {0}</p>", String.valueOf(downloadedImages)));

            Iterator<Page> iter = hashToPage.values().iterator();
            while (iter.hasNext())
            {
                Page page = iter.next();
                if (!page.succeeded)
                    continue;
                resultFile.newLine();
                resultFile.write(MessageFormat.format("<hr><h3>Page <small>[{0}]</small></h3>", page.hash));
                resultFile.newLine();
                resultFile.write(MessageFormat.format("<p>Images URL: {0}</p>", page.maskImagePartURL));
                resultFile.newLine();
                resultFile.write(
                        MessageFormat.format("<p>tile: {0},  z: {1},  x: {2},  y: {3}. Composite images: {4}</p>",
                                page.tile, page.z, page.x, page.y, page.parts));
                resultFile.newLine();
                resultFile.write(MessageFormat.format("<a href=\"{0}\">{0}</a></p>", page.url));
                resultFile.newLine();
                resultFile.write(MessageFormat.format(
                        "<figure><a href=\"{0}\"><img src=\"{1}\" alt=\"{0}\" ></a><figcaption><a href=\"{0}\">{0}</a></figcaption></figure>",
                        page.imageFileName, "thumbnail_" + page.imageFileName));
            }
            resultFile.newLine();
            resultFile.write("<hr><h1>Failed URLs</h1>");
            iter = hashToPage.values().iterator();
            while (iter.hasNext())
            {
                Page page = iter.next();
                if (page.succeeded)
                    continue;
                resultFile.newLine();
                resultFile.write(MessageFormat.format("<p>{0}</p>", page.url));
            }
            resultFile.newLine();
            resultFile.write("</body></html>");
            log("Result file complete.");
        }
        catch (Exception e)
        {
            log(e);
        }
        finally
        {
            if (resultFile != null)
            {
                try
                {
                    resultFile.flush();
                    resultFile.close();
                }
                catch (IOException e1)
                {
                    // nothing
                }
            }
        }
    }

    private String getHash(String s) throws NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.getBytes());
        byte hashMD5[] = md.digest();

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < hashMD5.length; i++)
        {
            sb.append(Integer.toString((hashMD5[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    /**
     * Log message.
     * 
     * @param msg
     */
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

    /**
     * Log message.
     * 
     * @param msg
     */
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

    private void finish()
    {
        writeResults();

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
            if (logFile != null)
            {
                try
                {
                    logFile.close();
                }
                catch (IOException e1)
                {
                    // nothing
                }
            }
        }
    }

    private static class Page
    {
        protected String hash;
        protected String url;
        protected String maskImagePartURL;
        protected String imageFileName;
        protected int tile;
        protected int z = -1;
        protected int y = -1;
        protected int x = -1;
        protected int parts = 0;
        protected BufferedImage image;
        protected boolean succeeded = false;
    }
}
