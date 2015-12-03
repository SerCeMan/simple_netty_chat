package ru.serce;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.GlobalEventExecutor;

public class OldStuff {
    public static void main(String[] args) throws InterruptedException {
        ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new ProtobufVarint32FrameDecoder());
                            p.addLast(new ProtobufDecoder(ChatProtocol.Message.getDefaultInstance()));

                            p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new ProtobufEncoder());

                            p.addLast(new SimpleChannelInboundHandler<ChatProtocol.Message>() {

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    channels.add(ctx.channel());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    channels.remove(ctx.channel());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ChatProtocol.Message msg) throws Exception {
                                    channels.stream()
                                            .filter(channel -> channel != ctx.channel())
                                            .forEach(chan -> chan.writeAndFlush(msg));
                                }
                            });
                        }
                    });

            b.bind(20053).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}