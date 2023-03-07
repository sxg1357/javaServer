import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisSlave {
    private static Map<String, String> kv = new HashMap<>();
    public static void main(String[] args) throws IOException {
        //创建监听socket
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(6380));
        serverChannel.configureBlocking(false);
        System.out.println("redis slave server is listening on 6380...");

        //连接redis服务器
        SocketChannel redisChannel = SocketChannel.open();
        redisChannel.connect(new InetSocketAddress("127.0.0.1", 6379));
        redisChannel.configureBlocking(false);
        //给服务器发送ping指令
        byte[] ping = "*1\r\n$4\r\nPING\r\n".getBytes();
        ByteBuffer bf = ByteBuffer.wrap(ping);
        redisChannel.write(bf);

        //创建epoll
        Selector selector = Selector.open();
        //将监听socket和连接redis服务器的socket添加到epoll事件管理
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        redisChannel.register(selector, SelectionKey.OP_READ);

        int i = 0;
        while (true) {
            selector.select();
            Set<SelectionKey> keys = selector.selectedKeys();
            for (SelectionKey key : keys) {
                if (key.isAcceptable()) {
                    System.out.println("客户端连接上来了");
                    ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                    SocketChannel clientChannel = channel.accept();
                    clientChannel.configureBlocking(false);
                    clientChannel.register(selector, SelectionKey.OP_READ);
                } else if (key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    //判断channel属于哪个socket 客户端的连接socket还是连接redis服务器的socket
                    if (channel.equals(redisChannel)) {
                        i++;
                        //redis服务器的channel
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        channel.read(buffer);
                        //buffer的当前位置更改为buffer缓冲区的第一个位置
                        buffer.flip();
                        String resp = new String(buffer.array(), 0, buffer.limit());
                        System.out.println("接收到来自6379的响应...");
                        System.out.println(resp);
                        if (i == 1) {
                            Pattern p = Pattern.compile("PONG");
                            Matcher m = p.matcher(resp);
                            if (m.find()) {
                                String in = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
                                ByteBuffer bf1 = ByteBuffer.wrap(in.getBytes());
                                channel.write(bf1);
                            }
                        }
                        String[] data = resp.split("\r\n");
                        if (data.length == 12) {
                            System.out.println("data[9]=" + data[9]);
                            System.out.println("data[11]=" + data[11]);
                            kv.put(data[9], data[11]);
                        }
                        if (i == 2) {
                            Pattern p = Pattern.compile("OK");
                            Matcher m = p.matcher(resp);
                            if (m.find()) {
                                String in = "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                                ByteBuffer bf2 = ByteBuffer.wrap(in.getBytes());
                                channel.write(bf2);
                            }
                        }
                        if (i == 3) {
                            Pattern p = Pattern.compile("OK");
                            Matcher m = p.matcher(resp);
                            if (m.find()) {
                                buffer.clear();
                                String in = "*3\r\n$5\r\nPSYNC\r\n$40\r\n209c52278f24ed2af8e9751be1dca563acf7f650\r\n$4\r\n4663\r\n";
                                ByteBuffer bf3 = ByteBuffer.wrap(in.getBytes());
                                channel.write(bf3);
                            }
                        }
                        if (i >= 4) {
                            buffer.clear();
                            String in = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$4\r\n4832\r\n";
                            ByteBuffer bf4 = ByteBuffer.wrap(in.getBytes());
                            channel.write(bf4);
                        }
                    } else {
                        //客户端的channel
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        channel.read(buffer);
                        //buffer的当前位置更改为buffer缓冲区的第一个位置
                        buffer.flip();
                        String data = new String(buffer.array(), 0, buffer.limit());
                        System.out.println("收到客户端的数据" + data);
                        String val = kv.get(data.trim());
                        ByteBuffer bf4;
                        if (val != null) {
                            bf4 = ByteBuffer.wrap(val.getBytes());
                        } else {
                            bf4 = ByteBuffer.wrap("val not found".getBytes());
                        }
                        channel.write(bf4);
                    }
                }
            }
            keys.clear();
        }
    }
}
