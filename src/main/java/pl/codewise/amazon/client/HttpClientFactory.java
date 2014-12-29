package pl.codewise.amazon.client;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;

public class HttpClientFactory {

	private static final HttpClientFactory INSTANCE = new HttpClientFactory();

	public AsyncHttpClient getHttpClient() {
		AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
				.setAllowPoolingConnections(true)
				.setConnectTimeout(1000)
				.setRequestTimeout(10000)
				.setFollowRedirect(false)
				.setMaxConnectionsPerHost(1000)
				.setMaxConnections(1000)
				.setIOThreadMultiplier(1)
				.build();

		return new AsyncHttpClient(config);
	}

	public static HttpClientFactory defaultFactory() {
		return INSTANCE;
	}

	public static HttpClientFactory supplierOf(AsyncHttpClient client) {
		return new HttpClientFactory() {
			@Override
			public AsyncHttpClient getHttpClient() {
				return client;
			}
		};
	}
}
