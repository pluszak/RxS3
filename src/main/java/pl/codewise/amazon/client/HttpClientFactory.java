package pl.codewise.amazon.client;

import pl.codewise.amazon.client.http.NettyHttpClient;

public class HttpClientFactory {

    private static final HttpClientFactory INSTANCE = new HttpClientFactory();

    public NettyHttpClient getHttpClient(ClientConfiguration configuration) {
        return new NettyHttpClient(configuration);
    }

    public static HttpClientFactory defaultFactory() {
        return INSTANCE;
    }
}
