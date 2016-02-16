package pl.codewise.amazon.client.http;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import pl.codewise.amazon.client.ClientConfiguration;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;
import pl.codewise.amazon.client.auth.Operation;

public class NettyHttpClient implements AutoCloseable {

    private final EventLoopGroup group;
    private final ChannelPool channelPool;

    private final String s3Location;

    public NettyHttpClient(ClientConfiguration configuration) {
        ThreadGroup threadGroup = new ThreadGroup("Netty RXS3 client");
        AtomicInteger threadCounter = new AtomicInteger();
        ThreadFactory threadFactory = r ->  new Thread(threadGroup, r, "Netty RXS3 client worker thread " + threadCounter.getAndIncrement());
        group = new NioEventLoopGroup(0, threadFactory);

        String[] s3LocationArray = configuration.getS3Location().trim().split(":");

        s3Location = s3LocationArray[0];
        int port = 80;
        if (s3LocationArray.length == 2) {
            port = Integer.parseInt(s3LocationArray[1]);
        }

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.getConnectionTimeoutMillis())
                .channel(NioSocketChannel.class)
                .remoteAddress(s3Location, port);

        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {

            HttpClientInitializer initializer = new HttpClientInitializer();

            @Override
            public void channelCreated(Channel ch) {
                initializer.initChannel(ch);
            }
        }, ChannelHealthChecker.ACTIVE, FixedChannelPool.AcquireTimeoutAction.FAIL,
                configuration.getAcquireTimeoutMillis(), configuration.getMaxConnections(), configuration.getMaxPendingAcquires());
    }

    public Request prepareGet(String url) {
        return new Request(url, Operation.GET);
    }

    public Request prepareList(String url) {
        return new Request(url, Operation.LIST);
    }

    public Request preparePut(String url) {
        return new Request(url, Operation.PUT);
    }

    public Request prepareDelete(String url) {
        return new Request(url, Operation.DELETE);
    }

    public <T> void executeRequest(Request requestData, SubscriptionCompletionHandler<T> completionHandler) {
        Future<Channel> acquire = channelPool.acquire();
        acquire.addListener(new RequestSender(s3Location, requestData, completionHandler, channelPool));
    }

    @Override
    public void close() {
        try {
            group.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            // ignore
        }
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
