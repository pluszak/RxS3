package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.timeout.IdleStateHandler;
import pl.codewise.amazon.client.InactiveConnectionsHandler;

class HttpClientInitializer {

    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final int MAX_REQUEST_SIZE = 1200 * BYTES_IN_MEGABYTE;

    private final HandlerDemultiplexer demultiplexer;

    HttpClientInitializer(HandlerDemultiplexer demultiplexer) {
        this.demultiplexer = demultiplexer;
    }

    void initChannel(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new IdleStateHandler(0, 0, 60));
        p.addLast(new HttpClientCodec());
        p.addLast(new HttpContentDecompressor());
        p.addLast(new HttpObjectAggregator(MAX_REQUEST_SIZE));
        p.addLast(demultiplexer);
        p.addLast(new InactiveConnectionsHandler());
    }
}
