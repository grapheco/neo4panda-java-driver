package org.grapheco.panda;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import java.util.HashMap;
import java.util.Map;

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

                String createFriendshipQuery =
                        "CREATE (p1:Person { name: $person1_name })\n" + "CREATE (p2:Person { name: $person2_name })\n"
                                + "CREATE (p1)-[k:KNOWS { from: $knows_from }]->(p2)\n"
                                + "RETURN p1, p2, k";

                Map<String, Object> params = new HashMap<>();
                params.put("person1_name", "Alice");
                params.put("person2_name", "David");
                params.put("knows_from", "School");
                Result result2 = tx.run(createFriendshipQuery, params);
                var r = result2.single();
                System.out.println(r.get(0));
                return result.single().get(0).asString();
            });
            System.out.println(greeting);
        }
    }

    public static void main(String... args) throws Exception {
        try (QuickStartDemo greeter = new QuickStartDemo("bolt://localhost:7600", "neo4j", "password")) {
            greeter.printGreeting("hello, world");
        }
    }
}