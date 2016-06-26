package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class HandlerDemultiplexer extends SimpleChannelInboundHandler<FullHttpResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandlerDemultiplexer.class);

    private static final AttributeKey<HttpClientHandler> HANDLER_ATTRIBUTE_KEY = AttributeKey.valueOf("handler");

    static final HandlerDemultiplexer INSTANCE = new HandlerDemultiplexer();

    private HandlerDemultiplexer() {
        super(false);
    }

    static void setAttributeValue(Channel channel, HttpClientHandler handler) {
        channel.attr(HANDLER_ATTRIBUTE_KEY).set(handler);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        HttpClientHandler httpClientHandler = ctx.channel().attr(HANDLER_ATTRIBUTE_KEY).get();
        if (httpClientHandler != null) {
            httpClientHandler.channelRead(ctx, msg);
        } else {
            LOGGER.error("No handler for channelRead0");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        HttpClientHandler httpClientHandler = ctx.channel().attr(HANDLER_ATTRIBUTE_KEY).get();
        if (httpClientHandler != null) {
            httpClientHandler.exceptionCaught(ctx, cause);
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        HttpClientHandler httpClientHandler = ctx.channel().attr(HANDLER_ATTRIBUTE_KEY).get();
        if (httpClientHandler != null) {
            httpClientHandler.channelInactive(ctx);
        } else {
            super.channelInactive(ctx);
        }
    }
}
