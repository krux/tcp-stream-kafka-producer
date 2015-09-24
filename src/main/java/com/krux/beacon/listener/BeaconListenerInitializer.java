package com.krux.beacon.listener;

import java.util.List;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import joptsimple.OptionSet;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 */
public class BeaconListenerInitializer extends ChannelInitializer<SocketChannel> {

    private static final StringDecoder DECODER = new StringDecoder();
    private static final StringEncoder ENCODER = new StringEncoder();

    private List<String> _topics;
    private int _decoderFrameSize;
    private OptionSet _options;

    public BeaconListenerInitializer(List<String> topics, int decoderFrameSize, OptionSet options) {
        _topics = topics;
        _decoderFrameSize = decoderFrameSize;
        _options = options;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        // Add the text line codec combination first,
        pipeline.addLast(new DelimiterBasedFrameDecoder(_decoderFrameSize, Delimiters.lineDelimiter()));
        // the encoder and decoder are static as these are sharable
        pipeline.addLast(DECODER);
        pipeline.addLast(ENCODER);

        // and then business logic.
        pipeline.addLast(new BeaconListenerHandler(_topics, _options));
    }
}