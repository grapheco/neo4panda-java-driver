/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.StdIOCapture;
import org.neo4j.driver.util.TestUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.neo4j.driver.Config.TrustStrategy.trustAllCertificates;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.TestUtil.await;

class ExamplesIT {
    static final String USER = "neo4j";

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private String uri;

    private int readInt(String database, final String query, final Value parameters) {
        SessionConfig sessionConfig;
        if (database == null) {
            sessionConfig = SessionConfig.defaultConfig();
        } else {
            sessionConfig = SessionConfig.forDatabase(database);
        }

        try (Session session = neo4j.driver().session(sessionConfig)) {
            return session.readTransaction(
                    tx -> tx.run(query, parameters).single().get(0).asInt());
        }
    }

    private int readInt(final String query, final Value parameters) {
        return readInt(null, query, parameters);
    }

    private int readInt(final String query) {
        return readInt(query, parameters());
    }

    private void write(final String query, final Value parameters) {
        try (Session session = neo4j.driver().session()) {
            session.writeTransaction(tx -> {
                tx.run(query, parameters).consume();
                return null;
            });
        }
    }

    private void write(String query) {
        write(query, parameters());
    }

    private int personCount(String name) {
        return readInt("MATCH (a:Person {name: $name}) RETURN count(a)", parameters("name", name));
    }

    private int companyCount(String name) {
        return readInt("MATCH (a:Company {name: $name}) RETURN count(a)", parameters("name", name));
    }

    @BeforeEach
    void setUp() {
        uri = neo4j.uri().toString();
        TestUtil.cleanDb(neo4j.driver());
    }

    @Test
    void testShouldRunAutocommitTransactionExample() throws Exception {
        // Given
        try (AutocommitTransactionExample example =
                new AutocommitTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");
            // Then
            assertTrue(personCount("Alice")>0);
        }
    }

    @Test
    void testShouldRunAsyncAutocommitTransactionExample() throws Exception {
        try (AsyncAutocommitTransactionExample example =
                new AsyncAutocommitTransactionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (Session session = neo4j.driver().session()) {
                session.run("UNWIND ['Tesseract', 'Orb', 'Eye of Agamotto'] AS item "
                        + "CREATE (:Product {id: 0, title: item})");
            }

            // read all 'Product' nodes
            List<String> titles = await(example.readProductTitles());
            assertEquals(new HashSet<>(asList("Tesseract", "Orb", "Eye of Agamotto")), new HashSet<>(titles));
        }
    }

    @Test
    void testShouldAsyncRunResultConsumeExample() throws Exception {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (AsyncResultConsumeExample example = new AsyncResultConsumeExample(uri, USER, neo4j.adminPassword())) {
            // When
            List<String> names = await(example.getPeople());

            // Then
            assertEquals(names, asList("Alice", "Bob"));
        }
    }

    @Test
    void testShouldAsyncRunMultipleTransactionExample() throws Exception {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (AsyncRunMultipleTransactionExample example =
                new AsyncRunMultipleTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            Integer nodesCreated = await(example.addEmployees("Acme"));

            // Then
            int employeeCount =
                    readInt("MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)");
            assertEquals(employeeCount, 2);
            assertEquals(nodesCreated, 1);
        }
    }

    @Test
    void testShouldRunConfigConnectionPoolExample() throws Exception {
        // Given
        try (ConfigConnectionPoolExample example = new ConfigConnectionPoolExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertTrue(example.canConnect());
        }
    }

    @Test
    void testShouldRunBasicAuthExample() throws Exception {
        // Given
        try (BasicAuthExample example = new BasicAuthExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertTrue(example.canConnect());
        }
    }

    @Test
    void testShouldRunHelloWorld() throws Exception {
        // Given
        try (HelloWorldExample greeter = new HelloWorldExample(uri, USER, neo4j.adminPassword())) {
            // When
            StdIOCapture stdIO = new StdIOCapture();

            try (AutoCloseable ignored = stdIO.capture()) {
                greeter.printGreeting("hello, world");
            }

            // Then
            assertEquals(stdIO.stdout().size(), 1);
            assertTrue(stdIO.stdout().get(0).contains("hello, world"));
        }
    }

    @Test
    void testShouldRunDriverIntroduction() throws Exception {
        // Given
        Config config = Config.builder()
                .withEncryption()
                .withTrustStrategy(trustAllCertificates())
                .build();
        try (DriverIntroductionExample intro =
                new DriverIntroductionExample(uri, USER, neo4j.adminPassword(), config)) {
            // When
            StdIOCapture stdIO = new StdIOCapture();

            try (AutoCloseable ignored = stdIO.capture()) {
                intro.createFriendship("Alice", "David", "School");
                intro.findPerson("Alice");
            }

            // Then
            assertEquals(stdIO.stdout().size(), 2);
            assertTrue(stdIO.stdout().get(0).contains("Created friendship between: Alice, David from School"));
            assertTrue(stdIO.stdout().get(1).contains("Found person: Alice"));
        }
    }

    @Test
    void testShouldRunReadWriteTransactionExample() throws Exception {
        // Given
        try (ReadWriteTransactionExample example = new ReadWriteTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            long nodeID = example.addPerson("Alice");

            // Then
            assertTrue(nodeID >= 0L);
        }
    }

    @Test
    void testShouldRunResultConsumeExample() throws Exception {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (ResultConsumeExample example = new ResultConsumeExample(uri, USER, neo4j.adminPassword())) {
            // When
            List<String> names = example.getPeople();

            // Then
            assertEquals(names, asList("Alice", "Bob"));
        }
    }

    @Test
    void testShouldRunResultRetainExample() throws Exception {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (ResultRetainExample example = new ResultRetainExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addEmployees("Acme");

            // Then
            int employeeCount =
                    readInt("MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)");
            assertEquals(employeeCount, 2);
        }
    }

    @Test
    void testShouldRunSessionExample() throws Exception {
        // Given
        try (SessionExample example = new SessionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertEquals(example.getClass(), SessionExample.class);
            assertTrue(personCount("Alice")> 0);
        }
    }

    @Test
    void testShouldRunTransactionFunctionExample() throws Exception {
        // Given
        try (TransactionFunctionExample example = new TransactionFunctionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    void testShouldConfigureTransactionTimeoutExample() throws Exception {
        // Given
        try (TransactionTimeoutConfigExample example =
                new TransactionTimeoutConfigExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");
            var pc = personCount("Alice");
            // Then
            assertTrue(pc > 0);
        }
    }

    @Test
    void testShouldConfigureTransactionMetadataExample() throws Exception {
        // Given
        try (TransactionMetadataConfigExample example =
                new TransactionMetadataConfigExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    void testShouldRunAsyncTransactionFunctionExample() throws Exception {
        try (AsyncTransactionFunctionExample example =
                new AsyncTransactionFunctionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (Session session = neo4j.driver().session()) {
                session.run(
                        "UNWIND ['Infinity Gauntlet', 'Mjölnir'] AS item " + "CREATE (:Product {id: 0, title: item})");
            }

            StdIOCapture stdIOCapture = new StdIOCapture();

            // print all 'Product' nodes to fake stdout
            try (AutoCloseable ignore = stdIOCapture.capture()) {
                ResultSummary summary = await(example.printAllProducts());
                assertEquals(QueryType.READ_ONLY, summary.queryType());
            }

            Set<String> capturedOutput = new HashSet<>(stdIOCapture.stdout());
            assertEquals(new HashSet<>(asList("Infinity Gauntlet", "Mjölnir")), capturedOutput);
        }
    }

    @Test
    void testPassBookmarksExample() throws Exception {
        try (PassBookmarkExample example = new PassBookmarkExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addEmployAndMakeFriends();

            // Then
            assertEquals(companyCount("Wayne Enterprises"), 1);
            assertEquals(companyCount("LexCorp"), 1);
            assertEquals(personCount("Alice"), 1);
            assertEquals(personCount("Bob"), 1);

            int employeeCountOfWayne = readInt(
                    "MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Wayne Enterprises' RETURN count(emp)");
            assertEquals(employeeCountOfWayne, 1);

            int employeeCountOfLexCorp = readInt(
                    "MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'LexCorp' RETURN count(emp)");
            assertEquals(employeeCountOfLexCorp, 1);

            int friendCount =
                    readInt("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN count(a)");
            assertEquals(friendCount, 1);
        }
    }

    @Test
    void testAsyncUnmanagedTransactionExample() throws Exception {
        try (AsyncUnmanagedTransactionExample example =
                new AsyncUnmanagedTransactionExample(uri, USER, neo4j.adminPassword())) {
            // create a 'Product' node
            try (Session session = neo4j.driver().session()) {
                session.run("CREATE (:Product {id: 0, title: 'Mind Gem'})");
            }

            StdIOCapture stdIOCapture = new StdIOCapture();

            // print the single 'Product' node
            try (AutoCloseable ignore = stdIOCapture.capture()) {
                await(example.printSingleProduct());
            }

            assertEquals(1, stdIOCapture.stdout().size());
            assertEquals("Mind Gem", stdIOCapture.stdout().get(0));
        }
    }

    @Test
    void testReadingValuesExample() throws Exception {
        try (ReadingValuesExample example = new ReadingValuesExample(uri, USER, neo4j.adminPassword())) {
            assertEquals(example.integerFieldIsNull(), false);
            assertEquals(example.integerAsInteger(), 4);
            assertEquals(example.integerAsLong(), 4L);
            assertThrows(Uncoercible.class, example::integerAsString);

            assertEquals(example.nullIsNull(), true);
            assertEquals(example.nullAsString(), "null");
            assertEquals(example.nullAsObjectFloatDefaultValue(), 1.0f);
            assertThrows(Uncoercible.class, example::nullAsObjectFloat);
        }
    }
}
