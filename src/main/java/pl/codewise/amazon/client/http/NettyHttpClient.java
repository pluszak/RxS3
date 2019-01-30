package pl.codewise.amazon.client.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import pl.codewise.amazon.client.ClientConfiguration;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;
import pl.codewise.amazon.client.auth.Operation;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyHttpClient implements AutoCloseable {

    private final String s3Location;
    private final EventLoopGroup group;

    private final HandlerDemultiplexer demultiplexer;
    private final ChannelPool channelPool;

    public NettyHttpClient(ClientConfiguration configuration) {
        ThreadGroup threadGroup = new ThreadGroup("Netty RxS3 client");
        AtomicInteger threadCounter = new AtomicInteger();
        ThreadFactory threadFactory = r -> new Thread(threadGroup, r, "RxS3-client-worker" + threadCounter.getAndIncrement());
        group = new NioEventLoopGroup(configuration.getWorkerThreadCount(), threadFactory);

        String[] s3LocationArray = configuration.getS3Location().trim().split(":");

        s3Location = s3LocationArray[0];
        int port;
        if (s3LocationArray.length == 2) {
            port = Integer.parseInt(s3LocationArray[1]);
        } else {
            port = 80;
        }

        demultiplexer = new HandlerDemultiplexer();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.getConnectionTimeoutMillis())
                .channel(NioSocketChannel.class)
                .remoteAddress(s3Location, port);

        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {

            HttpClientInitializer initializer = new HttpClientInitializer(demultiplexer);

            @Override
            public void channelCreated(Channel ch) {
                initializer.initChannel(ch);
            }
        }, ChannelHealthChecker.ACTIVE, FixedChannelPool.AcquireTimeoutAction.FAIL,
                configuration.getAcquireTimeoutMillis(), configuration.getMaxConnections(), configuration.getMaxPendingAcquires()) {
            @Override
            protected ChannelFuture connectChannel(Bootstrap bs) {
                bs.remoteAddress(s3Location, port);
                return super.connectChannel(bs);
            }
        };
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
        channelPool.acquire().addListener(new RequestSender(s3Location, requestData, completionHandler, demultiplexer, channelPool));
    }

    @Override
    public void close() {
        channelPool.close();
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
