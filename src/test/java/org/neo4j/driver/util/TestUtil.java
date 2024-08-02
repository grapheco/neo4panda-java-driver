package org.neo4j.driver.util;

import io.netty.util.internal.PlatformDependent;
import org.neo4j.driver.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestUtil {
    private static final long DEFAULT_WAIT_TIME_MS = MINUTES.toMillis(2);
    public static void cleanDb(Driver driver) {
        try (Session session = driver.session()) {
            int nodesDeleted;
            do {
                nodesDeleted = deleteBatchOfNodes(session);
            } while (nodesDeleted > 0);
        }
    }

    private static int deleteBatchOfNodes(Session session) {
        Result result = session.run("MATCH (n) WITH n LIMIT 1000 DETACH DELETE n RETURN count(n)");//TODO tx
        return result.single().get(0).asInt();
    }


    public static <T> T await(CompletionStage<T> stage) {
        return await((Future<T>) stage.toCompletableFuture());
    }

    public static <T> T await(CompletableFuture<T> future) {
        return await((Future<T>) future);
    }

    public static <T, U extends Future<T>> T await(U future) {
        try {
            return future.get(DEFAULT_WAIT_TIME_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future: " + future, e);
        } catch (ExecutionException e) {
            PlatformDependent.throwException(e.getCause());
            return null;
        } catch (TimeoutException e) {
            throw new AssertionError("Given future did not complete in time: " + future);
        }
    }
}
