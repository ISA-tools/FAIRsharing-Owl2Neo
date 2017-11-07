package org.fairsharing.owl2neo;

import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.Map;

public class Neo4jQuery {

    private static final int OK_STATUS = 0;
    private static final int ERR_STATUS = 1;

    protected static Options getOptions() {
        Option dbPath = new Option("d", "db-path", true, "The local location of the database");
        dbPath.setRequired(false);
        Options options = new Options();
        options.addOption(dbPath);
        return options;
    }

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = Utils.parseCommandLine(getOptions(), args);

        String graphDbPath = cmd.getOptionValue("d", Owl2Neo4jLoader.GRAPH_DB_PATH);
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphDbPath));
        String query = "MATCH (n) RETURN n ORDER BY ID(n) DESC LIMIT 30;";
        Transaction tx = graphDb.beginTx();
        try {
            Result result = graphDb.execute(query);
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (String key : result.columns()) {
                    Node node = (Node) row.get(key);
                    Object name = node.getProperty("name");
                    System.out.printf("%s = %s; name = %s%n", key, row.get(key), name);
                }

            }
            tx.success();
        }
        catch (Exception e) {
            System.err.println("Exception caught: " + e.getMessage());
            e.printStackTrace();
            System.exit(ERR_STATUS);
        }
        finally {
            tx.close();
        }
        System.out.println("Exiting with success...");
        System.exit(OK_STATUS);

    }

}
