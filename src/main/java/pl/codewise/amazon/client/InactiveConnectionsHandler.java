package pl.codewise.amazon.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InactiveConnectionsHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InactiveConnectionsHandler.class);

    public static final String NAME = "InactiveConnectionsHandler";

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOGGER.error("Channel became inactive!!!");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        LOGGER.error("Unexpected channel read invocation");
    }
}
