package pl.codewise.amazon.client.http;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class HttpClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientHandler.class);

    private final ChannelPool channelPool;
    private final SubscriptionCompletionHandler completionHandler;
    private boolean isKeepAlive;
    private boolean handlerNotified;

    public HttpClientHandler(ChannelPool channelPool, SubscriptionCompletionHandler completionHandler) {
        super(false);
        this.channelPool = channelPool;
        this.completionHandler = completionHandler;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        handlerNotified = true;
        if (!(isKeepAlive = HttpUtil.isKeepAlive(msg))) {
            ctx.close();
        } else {
            ctx.pipeline().remove(this);
            channelPool.release(ctx.channel());
        }
        completionHandler.onNext(msg);
        completionHandler.onCompleted();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        handlerNotified = true;
        LOGGER.error("Exception during request", cause);
        ctx.pipeline().remove(this);
        if (!isKeepAlive) {
            ctx.close();
        } else {
            channelPool.release(ctx.channel());
        }
        completionHandler.onError(cause);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (!handlerNotified) {
            handlerNotified = true;
            completionHandler.onError(new ChannelException("Channel become inactive"));
        }
    }
}
