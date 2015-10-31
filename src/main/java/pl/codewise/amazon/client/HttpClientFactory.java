package pl.codewise.amazon.client;

import pl.codewise.amazon.client.http.NettyHttpClient;

public class HttpClientFactory {

    private static final HttpClientFactory INSTANCE = new HttpClientFactory();

    public NettyHttpClient getHttpClient() {
//		AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
//				.setAllowPoolingConnections(true)
//				.setConnectTimeout(1000)
//				.setRequestTimeout(10000)
//				.setFollowRedirect(false)
//				.setMaxConnectionsPerHost(1000)
//				.setMaxConnections(1000)
//				.setIOThreadMultiplier(1)
//				.build();

        return new NettyHttpClient(100);
    }

    public static HttpClientFactory defaultFactory() {
        return INSTANCE;
    }

    public static HttpClientFactory supplierOf(NettyHttpClient client) {
        return new HttpClientFactory() {
            @Override
            public NettyHttpClient getHttpClient() {
                return client;
            }
        };
    }
}
