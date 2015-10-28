package pl.codewise.amazon.client.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class NettyHttpClient implements AutoCloseable {

    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    private final ChannelPool channelPool;

    public NettyHttpClient(int maxConnections) {
        group = new NioEventLoopGroup();

        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress("s3.amazonaws.com", 80)
                .handler(new HttpSnoopClientInitializer());

        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {

            @Override
            public void channelCreated(Channel ch) throws Exception {
                System.out.println("NettyHttpClient.channelCreated");
                ChannelPipeline p = ch.pipeline();

                p.addLast(new HttpClientCodec());
                p.addLast(new HttpContentDecompressor());

            }

            @Override
            public void channelAcquired(Channel ch) throws Exception {
                System.out.println("NettyHttpClient.channelAcquired");
            }

            @Override
            public void channelReleased(Channel ch) throws Exception {
                System.out.println("NettyHttpClient.channelReleased");
            }
        }, maxConnections);
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
        acquire.addListener(new GenericFutureListener<Future<? super Channel>>() {
            @Override
            public void operationComplete(Future<? super Channel> future) throws Exception {
                Channel ch = (Channel) future.getNow();

                DefaultFullHttpRequest request;
                if (requestData.getMethod().equals(HttpMethod.PUT)) {
                    request = new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1, requestData.getMethod(), requestData.getUrl(), requestData.getBody());
                } else {
                    request = new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1, requestData.getMethod(), requestData.getUrl());
                }

                request.headers().set(HttpHeaderNames.HOST, requestData.getBucketName() + ".s3.amazonaws.com");
                request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

                request.headers().set(HttpHeaderNames.CONTENT_TYPE, requestData.getContentType());
                request.headers().set(HttpHeaderNames.CONTENT_LENGTH, requestData.getContentLength());
                request.headers().set(HttpHeaderNames.CONTENT_MD5, requestData.getMd5());

                requestData.getSignatureCalculator().calculateAndAddSignature(request.headers(),
                        requestData.getUrl(), requestData.getMd5(), requestData.getContentType(), requestData.getBucketName());

                ch.pipeline().addLast(new HttpSnoopClientHandler<>(completionHandler));

                // Send the HTTP request.
                ch.writeAndFlush(request);
                channelPool.release(ch);
            }
        });
//        Channel ch = null;
//        try {
//            ch = bootstrap.connect("s3.amazonaws.com", 80).sync().channel();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }


//        // Wait for the server to close the connection.
//        try {
//            ch.closeFuture().sync();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }
}
