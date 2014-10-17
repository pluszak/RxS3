package pl.codewise.amazon.client;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.providers.netty.NettyAsyncHttpProviderConfig;

public class HttpClientFactory {

	private static final HttpClientFactory INSTANCE = new HttpClientFactory();

	public AsyncHttpClient getHttpClient() {
		NettyAsyncHttpProviderConfig providerConfig = new NettyAsyncHttpProviderConfig();
		providerConfig.addProperty(NettyAsyncHttpProviderConfig.REUSE_ADDRESS, true);

		AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
				.setAllowPoolingConnection(true)
				.setAsyncHttpClientProviderConfig(providerConfig)
				.setConnectionTimeoutInMs(1000)
				.setRequestTimeoutInMs(10000)
				.setFollowRedirects(false)
				.setMaximumConnectionsPerHost(1000)
				.setMaximumConnectionsTotal(1000)
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
