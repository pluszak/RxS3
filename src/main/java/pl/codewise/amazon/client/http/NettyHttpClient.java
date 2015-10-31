package pl.codewise.amazon.client.http;

import java.lang.reflect.Field;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.concurrent.Future;
import pl.codewise.amazon.client.ClientConfiguration;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class NettyHttpClient implements AutoCloseable {

    private final EventLoopGroup group;
    private final ChannelPool channelPool;

    public NettyHttpClient(ClientConfiguration configuration) {
        group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.getConnectionTimeoutMillis())
                .channel(NioSocketChannel.class)
                .remoteAddress(configuration.getS3Location(), 80);

        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {

            HttpClientInitializer initializer = new HttpClientInitializer();

            @Override
            public void channelCreated(Channel ch) {
                initializer.initChannel(channelPool, ch);
            }
        }, ChannelHealthChecker.ACTIVE, FixedChannelPool.AcquireTimeoutAction.FAIL, 0, configuration.getMaxConnections(), 1, true);
    }

    public Request prepareGet(String url) {
        return new Request(url, HttpMethod.GET);
    }

    public Request preparePut(String url) {
        return new Request(url, HttpMethod.PUT);
    }

    public Request prepareDelete(String url) {
        return new Request(url, HttpMethod.DELETE);
    }

    public <T> void executeRequest(Request requestData, SubscriptionCompletionHandler<T> completionHandler) {
        Future<Channel> acquire = channelPool.acquire();
        acquire.addListener(new RequestSender(requestData, completionHandler));
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }

    public int acquiredConnections() {
        try {
            Field acquiredChannelCount = FixedChannelPool.class.getDeclaredField("acquiredChannelCount");
            acquiredChannelCount.setAccessible(true);

            return (int) acquiredChannelCount.get(channelPool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
