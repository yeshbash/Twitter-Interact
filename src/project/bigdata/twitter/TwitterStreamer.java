package project.bigdata.twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamer implements Runnable {

	private static String CONSUMER_KEY = "XX";
	private static String CONSUMER_TOKEN = "XX";
	private static String TOKEN_KEY = "XX";
	private static String TOKEN_SECRET = "XX";

	private BlockingQueue<String> target;
	private List<String> queries;
	private String threadName;

	public TwitterStreamer(String threadName, BlockingQueue<String> target,
			List<String> queries) {
		super();
		this.target = target;
		this.threadName = threadName;
		this.queries = queries;
	}

	@Override
	public void run() {
		Client client = null;
		try {
			Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_TOKEN,
					TOKEN_KEY, TOKEN_SECRET);
			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint()
					.trackTerms(this.queries);
			BlockingQueue<String> twitterStreamQ = new LinkedBlockingQueue<String>();
			endpoint.addPostParameter("language", "en");

			ClientBuilder clientBuilder = new ClientBuilder()
					.hosts(Constants.STREAM_HOST).authentication(auth)
					.endpoint(endpoint)
					.processor(new StringDelimitedProcessor(twitterStreamQ));

			client = clientBuilder.build();
			client.connect();
			int count = 0;
			while (count++ < 5) {
				String message = twitterStreamQ.take();
				JsonParser p = new JsonParser();
				JsonElement tweetElement = p.parse(message);
				if (tweetElement.isJsonObject()) {
					JsonObject tweet = tweetElement.getAsJsonObject();
					System.out.printf("[%s] Tweet : %s\n", this.threadName,
							tweet.get("text"));
				}

			}
		} catch (Exception e) {
			System.out.printf("[%s] Exception :%s", threadName, e.getMessage());
		} finally {
			if (client != null) {
				client.stop();
			}
		}

	}

}
