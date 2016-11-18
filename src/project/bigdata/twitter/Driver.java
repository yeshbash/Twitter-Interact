package project.bigdata.twitter;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Driver {
	public static void main(String[] args) {

		ExecutorService execService = null;
		int params = 0;
		try {
			if (args != null) {
				params = args.length;
				int counter = 0;
				execService = Executors.newFixedThreadPool(params);
				while (counter < params) {
					List<String> queryString = Arrays.asList(args[counter].split("-"));
					TwitterStreamer streamer = new TwitterStreamer("Thread"+ counter
											,new LinkedBlockingQueue<String>(10),queryString);
					execService.execute(streamer);
					counter++;
				}
				//System.out.println("Outside Loop");
			}

		} catch (Exception e) {
			e.printStackTrace();
			//System.out.println("Failing");
		} finally {
			execService.shutdown();
		}

	}
}
