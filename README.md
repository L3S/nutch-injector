Nutch Injector
==============

This project provides a simple way to add new seed URLs to a Nutch crawl, 
bypassing the standard InjectorJob. Additionally, it adds the possibility to
store redirections in the crawl DB.

The original use case is to crawl links found in a Twitter stream using Nutch.
In this scenario, we continously get new URLs, which might have been crawled
earlier already. Additionally, all links in a tweet are available as a 
shortened link (t.co/...) and the original link. We want to insert this 
relation into the crawl DB to capture the relation between the two URLs and
to avoid re-crawling the same URL.


Usage
-----

```java
	Injector injector = new Injector(conf[, crawlId]);
	
	injector.inject("http://www.l3s.de/");
	injector.addRedirect("http://t.co/ZNyOoEwAwN", "http://www.l3s.de/");
	
	Map<String, String> metadata = new HashMap<>();
	metadata.add("source", "#l3s");
	injector.inject("http://www.l3s.de/", metadata);
```

License
-------

This code can be used under the Apache License Version 2.0 (see 
http://www.apache.org/licenses/).