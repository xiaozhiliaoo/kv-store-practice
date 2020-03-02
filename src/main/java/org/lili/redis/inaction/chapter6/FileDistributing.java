package org.lili.redis.inaction.chapter6;

import org.lili.redis.inaction.base.Base;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static java.lang.Thread.sleep;

/**
 * @author lili
 * @date 2020/3/1 22:26
 * @description
 * @notes
 */
public class FileDistributing extends Base {

    public void copyLogsThread(File path, String channel, int count, long limit) {
        Deque<File> waiting = new ArrayDeque<File>();
        long bytesInRedis = 0;
        ChatManager chatManager = new ChatManager();
        Set<String> recipients = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            recipients.add(String.valueOf(i));
        }
        chatManager.createChat("source", recipients, "", channel);
        File[] logFiles = path.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("temp_redis");
            }
        });
        Arrays.sort(logFiles);
        for (File logFile : logFiles) {
            long fsize = logFile.length();
            while ((bytesInRedis + fsize) > limit) {
                long cleaned = clean(channel, waiting, count);
                if (cleaned != 0) {
                    bytesInRedis -= cleaned;
                } else {
                    try {
                        sleep(250);
                    } catch (InterruptedException ie) {
                        Thread.interrupted();
                    }
                }
            }

            BufferedInputStream in = null;
            try {
                in = new BufferedInputStream(new FileInputStream(logFile));
                int read = 0;
                byte[] buffer = new byte[8192];
                while ((read = in.read(buffer, 0, buffer.length)) != -1) {
                    if (buffer.length != read) {
                        byte[] bytes = new byte[read];
                        System.arraycopy(buffer, 0, bytes, 0, read);
                        client.append((channel + logFile).getBytes(), bytes);
                    } else {
                        client.append((channel + logFile).getBytes(), buffer);
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw new RuntimeException(ioe);
            } finally {
                try {
                    in.close();
                } catch (Exception ignore) {
                }
            }

            chatManager.sendMessage(channel, "source", logFile.toString());

            bytesInRedis += fsize;
            waiting.addLast(logFile);
        }

        chatManager.sendMessage(channel, "source", ":done");

        while (waiting.size() > 0) {
            long cleaned = clean(channel, waiting, count);
            if (cleaned != 0) {
                bytesInRedis -= cleaned;
            } else {
                try {
                    sleep(250);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }
            }
        }

    }

    private long clean(String channel, Deque<File> waiting, int count) {
        if (waiting.size() == 0) {
            return 0;
        }
        File w0 = waiting.getFirst();
        if (String.valueOf(count).equals(client.get(channel + w0 + ":done"))) {
            client.del(channel + w0, channel + w0 + ":done");
            return waiting.removeFirst().length();
        }
        return 0;
    }

    public interface Callback {
        void callback(String line);
    }


    public void processLogsFromRedis(String id, Callback callback) throws InterruptedException, IOException {
        ChatManager chatManager = new ChatManager();
        while (true) {
            List<ChatManager.ChatMessages> fdata = chatManager.fetchPendingMessages(id);

            for (ChatManager.ChatMessages messages : fdata) {
                for (Map<String, Object> message : messages.messages) {
                    String logFile = (String) message.get("message");

                    if (":done".equals(logFile)) {
                        return;
                    }
                    if (logFile == null || logFile.length() == 0) {
                        continue;
                    }

                    InputStream in = new RedisInputStream(messages.chatId + logFile);
                    if (logFile.endsWith(".gz")) {
                        in = new GZIPInputStream(in);
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            callback.callback(line);
                        }
                        callback.callback(null);
                    } finally {
                        reader.close();
                    }

                    client.incr(messages.chatId + logFile + ":done");
                }
            }

            if (fdata.size() == 0) {
                Thread.sleep(100);
            }
        }
    }



    public class RedisInputStream extends InputStream {
        private String key;
        private int pos;

        public RedisInputStream(String key) {
            this.key = key;
        }

        @Override
        public int available()
                throws IOException {
            long len = client.strlen(key);
            return (int) (len - pos);
        }

        @Override
        public int read() throws IOException {
            byte[] block = client.substr(key.getBytes(), pos, pos);
            if (block == null || block.length == 0) {
                return -1;
            }
            pos++;
            return (int) (block[0] & 0xff);
        }

        @Override
        public int read(byte[] buf, int off, int len)
                throws IOException {
            byte[] block = client.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0) {
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        @Override
        public void close() {
            // no-op
        }
    }

}
