package pl.kwiatek.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import pl.kwiatek.redis.RedisSubscribingThread;

/**
 * Hello world!
 *
 */
public class RedisMain
{
    private static final String CHANNEL = "test-run-cancels";

    public static void main( String[] args ) throws InterruptedException {
        RedisClient redisClient = RedisClient.create("redis://localhost");
        RedisSubscribingThread redisSubscribingThread = new RedisSubscribingThread(redisClient, CHANNEL);
        redisSubscribingThread.start();
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisCommands<String, String> syncCommands = connection.sync();
            syncCommands.publish(CHANNEL, "test message");
            Thread.sleep(5000L);
        }
        redisSubscribingThread.shutDown();
        redisSubscribingThread.join();
        redisClient.shutdown();

        System.out.println("Complete");
    }
}
