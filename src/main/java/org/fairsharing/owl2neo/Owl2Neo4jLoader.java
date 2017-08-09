package org.fairsharing.owl2neo;

import openllet.owlapi.OpenlletReasonerFactory;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import org.semanticweb.owlapi.util.OWLAPIStreamUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.Map;

public class Owl2Neo4jLoader {

    private static final int OK_STATUS = 0;
    private static final int ERR_STATUS = 1;

    private GraphDatabaseService graphDb;

    @Inject
    public Owl2Neo4jLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    public void setGraphDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    private Node getOrCreateWithUniqueFactory(String nodeName) {

        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, "index") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("name", properties.get("name"));
            }
        };

        return factory.getOrCreate("name", nodeName);
    }

    public void importOntology(OWLOntology ontology) throws Exception {
        final OWLReasoner reasoner = OpenlletReasonerFactory.getInstance().createReasoner(ontology);
        if (!reasoner.isConsistent()) {
            throw new Exception("Ontology is inconsistent");
        }
        Transaction tx = graphDb.beginTx();
        try {
            Node thingNode = getOrCreateWithUniqueFactory("owl:thing");
            for (OWLClass c : OWLAPIStreamUtils.asList(ontology.classesInSignature())) {
                String classString = c.toString();
                System.out.println("Current OWL class is: " + classString);
            }
        }
        finally {
            tx.close();
        }

    }

    protected static Options getOptions() {
        Option ontologyPath = new Option("o", "ontology-path", true, "The local location of the OWL file");
        ontologyPath.setRequired(true);
        Options options = new Options();
        options.addOption(ontologyPath);
        return options;
    }

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd =  parser.parse(getOptions(), args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(Owl2Neo4jLoader.class.getSimpleName(), getOptions());
            System.exit(ERR_STATUS);
        }
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();


        File file = new File(cmd.getOptionValue("o").trim());
        try {
            OWLOntology ontology = manager.loadOntologyFromOntologyDocument(file);
            System.out.println("Loaded ontology" + ontology);
            GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("var/graphdb"));
            Owl2Neo4jLoader loader = new Owl2Neo4jLoader(graphDb);
            loader.importOntology(ontology);
            System.exit(OK_STATUS);
        }
        catch (Exception e) {
            System.exit(ERR_STATUS);
        }

    }


}
