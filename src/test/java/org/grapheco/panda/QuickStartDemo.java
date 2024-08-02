package org.grapheco.panda;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import static org.neo4j.driver.Values.parameters;

public class QuickStartDemo implements AutoCloseable {
    private final Driver driver;

    public QuickStartDemo(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void printGreeting(final String message) {
        try (Session session = driver.session()) {
            String greeting = session.writeTransaction(tx -> {
                Result result = tx.run(
                        "CREATE (a:Greeting) " + "SET a.message = $message "
                                + "RETURN a.message + ', from node ' + id(a)",
                        parameters("message", message));
                return result.single().get(0).asString();
            });
            System.out.println(greeting);
        }
    }

    public static void main(String... args) throws Exception {
        try (QuickStartDemo greeter = new QuickStartDemo("bolt://10.0.82.146:7600", "neo4j", "password")) {
            greeter.printGreeting("hello, world");
        }
    }
}