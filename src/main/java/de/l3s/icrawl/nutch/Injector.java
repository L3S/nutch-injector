package de.l3s.icrawl.nutch;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Write data to a Nutch 2.x CrawlDB.
 *
 * For now, this skips any configured URL filters and URL normalizers.
 */
public class Injector implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Injector.class);
    private static final Utf8 ZERO_STRING = new Utf8("0");
    private static final Utf8 YES_STRING = new Utf8("y");
    private final DataStore<String, WebPage> store;
    /** Score for injected pages, unless overridden my metadata. */
    private final float defaultScore;
    /** Fetch interval for injected pages, unless overridden my metadata. */
    private final int defaultInterval;

    /**
     * Create injector for the default CrawlDB.
     *
     * @param conf
     *            the configuration for Nutch & Hadoop
     * @throws InjectorSetupException
     *             if we cannot connect to the CrawlDB
     */
    public Injector(Configuration conf) throws InjectorSetupException {
        this(conf, "");
    }

    /**
     * Create injector for the named crawl.
     *
     * @param conf
     *            the configuration for Nutch & Hadoop
     * @param crawlId
     *            a string refering to an existing CrawlDB.
     * @throws InjectorSetupException
     *             if we cannot connect to the CrawlDB
     */
    public Injector(Configuration conf, String crawlId) throws InjectorSetupException {
        this(conf, createStore(conf, crawlId));
    }

    /**
     * Create injector for the given CrawlDB,
     *
     * @param conf
     *            the configuration for Nutch & Hadoop
     * @param store
     *            connection to the CrawlDB
     * @throws InjectorSetupException
     */
    public Injector(Configuration conf, DataStore<String, WebPage> store)
            throws InjectorSetupException {
        this.store = store;
        defaultScore = conf.getFloat("db.score.injected", 1.0f);
        defaultInterval = conf.getInt("db.fetch.interval.default", 2592000);
    }

    /** Create datastore for crawlId and wrap thrown exceptions. */
    public static DataStore<String, WebPage> createStore(Configuration conf, String crawlId)
            throws InjectorSetupException {
        try {
            conf.set(Nutch.CRAWL_ID_KEY, crawlId);
            return StorageUtils.createWebStore(conf, String.class, WebPage.class);
        } catch (ClassNotFoundException e) {
            throw new InjectorSetupException("Missing class for Gora storage", e);
        } catch (GoraException e) {
            throw new InjectorSetupException("Could not get connection for crawl " + crawlId, e);
        }
    }

    /**
     * Test if a given URL is in the CrawlDB.
     *
     * @param url
     *            the URL to check
     * @return true, iff an entry for the URL is stored in the DB
     */
    public boolean hasUrl(String url) {
        try {
            String key = TableUtil.reverseUrl(url);
            WebPage webPage = store.get(key, array(WebPage.Field.STATUS.getName()));
            return webPage != null;
        } catch (NullPointerException e) { // MemStore.get fails on missing keys
            return false;
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Not a valid URL: " + url, e);
        }
    }

    /**
     * Add a new seed URL to the CrawlDB, if it doesn't exist yet.
     *
     * @param url
     *            the URL to add
     * @return false, if the URL is already in the DB, true otherwise
     * @throws InjectorInjectionException
     *             when url is not a valid URL
     */
    public boolean inject(String url) throws InjectorInjectionException {
        return inject(url, Collections.<String, String> emptyMap());
    }

    /**
     * Add a new seed URL to the CrawlDB, if it doesn't exist yet.
     *
     * @param url
     *            the URL to add
     * @param metadata
     *            additional metadata to store in the DB. InjectorJob metadata
     *            (nutch.score and nutch.fetchInterval) is handled correctly.
     * @return false, if the URL is already in the DB, true otherwise
     * @throws InjectorInjectionException
     *             when url is not a valid URL
     */
    public boolean inject(String url, Map<String, String> metadata) throws InjectorInjectionException {
        if (hasUrl(url)) {
            return false;
        }
        WebPage webPage = createSeedWebPage(url, metadata);

        putRow(url, webPage, true);
        return true;
    }

    /**
     * Save a row in the store.
     *
     * @param url
     *            the row key
     * @param webPage
     *            the row value
     * @param flush
     *            if true, force a write afterwards
     */
    private void putRow(String url, WebPage webPage, boolean flush) throws InjectorInjectionException {
        String rowKey;
        try {
            rowKey = TableUtil.reverseUrl(url);
        } catch (MalformedURLException e) {
            throw new InjectorInjectionException(e);
        }
        store.put(rowKey, webPage);
        if (flush) {
            store.flush();
        }
    }

    /**
     * Store a redirect in the crawl DB, unless there is already an entry.
     *
     * @param fromUrl
     *            the redirecting URL
     * @param toUrl
     *            the redirect target URL
     * @return false, if the redirecting URL is already stored
     * @throws InjectorInjectionException
     *             when one of the URLs is not valid
     */
    public boolean addRedirect(String fromUrl, String toUrl) throws InjectorInjectionException {
        return addRedirect(fromUrl, toUrl, Collections.<String, String> emptyMap());
    }

    /**
     * Store a redirect in the crawl DB, unless there is already an entry.
     *
     * @param fromUrl
     *            the redirecting URL
     * @param toUrl
     *            the redirect target URL
     * @param metadata
     *            additional metadata. The data is added to all created rows
     * @return false, if the redirecting URL is already stored
     * @throws InjectorInjectionException
     *             when one of the URLs is not valid
     */
    public boolean addRedirect(String fromUrl, String toUrl, Map<String, String> metadata)
            throws InjectorInjectionException {
        if (hasUrl(fromUrl)) { // TODO maybe also check if URL has already been fetched
            return false;
        }
        WebPage redirect = createRedirectWebPage(fromUrl, toUrl, metadata);
        putRow(fromUrl, redirect, false);
        inject(toUrl, metadata);
        return true;
    }

    /** Create WebPage object for redirecting URL. */
    private WebPage createRedirectWebPage(String from, String to, Map<String, String> metadata) {
        WebPage page = new WebPage();
        page.putToOutlinks(new Utf8(to), new Utf8());
        page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
        page.setReprUrl(new Utf8(to));
        page.setFetchTime(System.currentTimeMillis());
        page.setProtocolStatus(ProtocolStatusUtils.makeStatus(ProtocolStatusCodes.MOVED, to));
        page.setStatus(CrawlStatus.STATUS_REDIR_PERM);
        return page;
    }

    /** Create WebPage object for a seed URL. */
    private WebPage createSeedWebPage(String url, Map<String, String> metadata) {
        WebPage webPage = new WebPage();
        float score = defaultScore;
        int interval = defaultInterval;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (InjectorJob.nutchScoreMDName.equals(key)) {
                try {
                    score = Float.parseFloat(value);
                } catch (NumberFormatException e) {
                    logger.debug("Got illegal score value '{}' for URL '{}', ignoring", value, url);
                }
            } else if (InjectorJob.nutchFetchIntervalMDName.equals(key)) {
                try {
                    interval = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    logger.debug("Got illegal fetch interval value '{}' for URL '{}', ignoring",
                        value, key);
                }
            }
            Utf8 writtenKey = new Utf8(key);
            ByteBuffer writtenValue = ByteBuffer.wrap(value.getBytes(UTF_8));
            webPage.putToMetadata(writtenKey, writtenValue);
        }
        webPage.setScore(score);
        webPage.setFetchInterval(interval);
        webPage.setFetchTime(System.currentTimeMillis());
        webPage.putToMarkers(DbUpdaterJob.DISTANCE, ZERO_STRING);
        Mark.INJECT_MARK.putMark(webPage, YES_STRING);
        return webPage;
    }

    /** Collect arguments in a String[]. */
    private static String[] array(String... values) {
        return values;
    }

    /**
     * Write a document to the CrawlDB.
     *
     * If the document has been stored already, it is not overwritten.The fields
     * <code>prevFetchTime</code> and <code>protocolStatus</code> are not
     * handled yet.
     *
     * @param url
     *            the URL of the web page, also used as the <code>baseUrl</code>
     * @param content
     *            the actual content of the page
     * @param contentType
     *            MIME type of the content
     * @param headers
     *            protocol headers
     * @param metadata
     *            additional metadata
     * @param batchId
     *            the batch this document is associated with, used by other
     *            Nutch jobs
     * @return true if the document was inserted, false if there is a document
     *         for this URL already
     * @throws InjectorInjectionException
     *             when the URL is invalid
     */
    public boolean writeDocument(String url, byte[] content, String contentType,
            Map<String, String> headers, Map<String, String> metadata, String batchId)
            throws InjectorInjectionException {
        if (hasUrl(url)) {
            return false;
        }
        WebPage page = new WebPage();
        page.setStatus(CrawlStatus.STATUS_FETCHED);
        page.setFetchTime(System.currentTimeMillis());

        if (content != null) {
            page.setContent(ByteBuffer.wrap(content));
            page.setContentType(new Utf8(contentType));
            page.setBaseUrl(new Utf8(url));
        }

        if (metadata != null) {
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                Utf8 key = new Utf8(entry.getKey());
                ByteBuffer value = ByteBuffer.wrap(entry.getValue().getBytes(UTF_8));
                page.putToMetadata(key, value);
            }
        }

        page.putToMarkers(DbUpdaterJob.DISTANCE, ZERO_STRING);

        Mark.FETCH_MARK.putMark(page, batchId);

        putRow(url, page, true);
        return true;
    }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
