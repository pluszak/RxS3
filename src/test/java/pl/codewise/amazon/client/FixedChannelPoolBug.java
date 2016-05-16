package pl.codewise.amazon.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FixedChannelPoolBug {

    public static final int MAX_CONNECTIONS = 1;
    public static final int PORT = 8080;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @BeforeMethod
    public void setUp() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpRequestDecoder());
                        p.addLast(new HttpResponseEncoder());
                    }
                });

        b.bind(PORT).sync().channel();
    }

    @AfterMethod
    public void tearDown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Test
    public void shouldHang() throws InterruptedException {
        // Given
        NioEventLoopGroup group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress("127.0.0.1", 8080);

        ChannelHealthChecker checker = channel ->
                channel.eventLoop().newSucceededFuture(Boolean.FALSE);

        FixedChannelPool channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {

            @Override
            public void channelCreated(Channel ch) {
                // Channel initialization goes here
            }
        }, checker, FixedChannelPool.AcquireTimeoutAction.FAIL, 1000, MAX_CONNECTIONS, 1);

        //When
        for (int i = 0; i < 2 * MAX_CONNECTIONS; i++) {
            Future<Channel> channelFuture = channelPool.acquire().addListener(future -> {
                if (future.isSuccess()) {
                    System.out.println("Channel acquired");
                    channelPool.release((Channel) future.getNow());
                } else {
                    System.out.println("Channel acquisition failed");
                }
            });

            channelFuture.sync();
        }

        // Then
        // :(
    }
}
