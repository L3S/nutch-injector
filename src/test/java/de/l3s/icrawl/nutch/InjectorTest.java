package de.l3s.icrawl.nutch;

import org.apache.gora.memory.store.MemStore;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@Ignore
public class InjectorTest {

    private static final String SHORT_URL = "http://t.co/ZNyOoEwAwN";
    private static final String URL = "http://www.l3s.de/";
    private Injector injector;
    private DataStore<String, WebPage> store;

    @Before
    public void testConstructor() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("storage.data.store.class", MemStore.class.getName());
        store = Injector.createStore(conf, "");
        injector = new Injector(conf, store);
    }

    @Test
    public void testHasUrl_false() {
        boolean hasUrl = injector.hasUrl(URL);
        assertFalse("doesn't have URL", hasUrl);
    }

    @Test
    public void testHasUrl_true() throws Exception {
        store.put(TableUtil.reverseUrl(URL), WebPage.newBuilder().build());
        boolean hasUrl = injector.hasUrl(URL);
        assertTrue("has URL", hasUrl);
    }

    @Test
    public void testInjectUrl() throws Exception {
        assumeFalse(injector.hasUrl(URL));
        boolean injected = injector.inject(URL);
        assertTrue(injected);
        assertTrue(injector.hasUrl(URL));
    }

    @Test
    public void testAddRedirect() throws Exception {
        assumeFalse(injector.hasUrl(URL));
        assumeFalse(injector.hasUrl(SHORT_URL));
        boolean added = injector.addRedirect(SHORT_URL, "http://www.l3s.de/");
        assertTrue(added);
        assertTrue(injector.hasUrl(URL));
        assertTrue(injector.hasUrl(SHORT_URL));
    }

}
