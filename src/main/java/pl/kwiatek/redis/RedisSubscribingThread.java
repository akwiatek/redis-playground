package pl.kwiatek.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisSubscribingThread extends Thread {

    private final RedisClient redisClient;
    private final String[] channels;

    private volatile boolean shuttingDown;

    public RedisSubscribingThread(RedisClient redisClient, String... channels) {
        this.redisClient = redisClient;
        this.channels = channels;
    }

    public void shutDown() {
        shuttingDown = true;
        interrupt();
    }

    public void run() {
        try (StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub()) {
            connection.addListener(new LoggingRedisPubSubAdapter<>());
            connection.sync().subscribe(channels);
            sleepUntilShutDown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sleepUntilShutDown() throws InterruptedException {
        while (!shuttingDown) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                if (shuttingDown) {
                    break;
                }
                throw e;
            }
        }
    }

    private static class LoggingRedisPubSubAdapter<K, V> extends RedisPubSubAdapter<K, V> {
        @Override
        public void message(K channel, V message) {
            System.out.println("onMessage, channel:" + channel + ", message: " + message);
        }
    }
}
