package de.l3s.icrawl.nutch;

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

public class Injector {
    private static final Logger logger = LoggerFactory.getLogger(Injector.class);
    private static final Utf8 ZERO_STRING = new Utf8("0");
    private static final Utf8 YES_STRING = new Utf8("y");
    private final DataStore<String, WebPage> store;
    private final float defaultScore;
    private final int defaultInterval;

    public Injector(Configuration conf) throws InjectorSetupException {
        this(conf, "");
    }

    public Injector(Configuration conf, String crawlId) throws InjectorSetupException {
        this(conf, createStore(conf, crawlId));
    }

    public Injector(Configuration conf, DataStore<String, WebPage> store)
            throws InjectorSetupException {
        this.store = store;
        defaultScore = conf.getFloat("db.score.injected", 1.0f);
        defaultInterval = conf.getInt("db.fetch.interval.default", 2592000);
    }

    static DataStore<String, WebPage> createStore(Configuration conf, String crawlId)
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

    public boolean inject(String url) throws InjectorInjectionException {
        return inject(url, Collections.<String, String> emptyMap());
    }

    public boolean inject(String url, Map<String, String> metadata) throws InjectorInjectionException {
        if (hasUrl(url)) {
            return false;
        }
        WebPage webPage = createWebPage(url, metadata);

        putRow(url, webPage, true);
        return true;
    }

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

    public boolean addRedirect(String fromUrl, String toUrl) throws InjectorInjectionException {
        return addRedirect(fromUrl, toUrl, Collections.<String, String> emptyMap());
    }

    public boolean addRedirect(String from, String to, Map<String, String> metadata)
            throws InjectorInjectionException {
        if (hasUrl(from)) {
            return false;
        }
        WebPage redirect = createRedirect(from, to, metadata);
        putRow(from, redirect, false);
        inject(to, metadata);
        return true;
    }

    private WebPage createRedirect(String from, String to, Map<String, String> metadata) {
        WebPage page = new WebPage();
        page.putToOutlinks(new Utf8(to), new Utf8());
        page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
        page.setReprUrl(new Utf8(to));
        page.setFetchTime(System.currentTimeMillis());
        page.setProtocolStatus(ProtocolStatusUtils.makeStatus(ProtocolStatusCodes.MOVED, to));
        page.setStatus(CrawlStatus.STATUS_REDIR_PERM);
        return page;
    }

    private WebPage createWebPage(String url, Map<String, String> metadata) {
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

    private static String[] array(String... values) {
        return values;
    }
}
