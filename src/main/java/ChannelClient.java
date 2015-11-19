import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.io.IOException;
import java.util.Scanner;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChannelClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();

                            p.addLast(new ProtobufVarint32FrameDecoder());
                            p.addLast(new ProtobufDecoder(ChatProtocol.Message.getDefaultInstance()));

                            p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new ProtobufEncoder());

                            p.addLast(new SimpleChannelInboundHandler<ChatProtocol.Message>() {
                                @Override
                                protected void messageReceived(ChannelHandlerContext ctx, ChatProtocol.Message msg) throws Exception {
                                    System.out.printf("%s: %s%n", msg.getAuthor(), msg.getText());
                                }
                            });
                        }
                    });

            // Make a new connection.
            Channel ch = b.connect("localhost", 20026).sync().channel();

            Scanner scanner = new Scanner(System.in);
            while (true) {
                String msg = scanner.nextLine();
                ch.writeAndFlush(ChatProtocol.Message.newBuilder()
                        .setType(ChatProtocol.Message.Type.MESSAGE)
                        .setAuthor(Thread.currentThread().getName())
                        .setText(msg)
                        .build());
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}