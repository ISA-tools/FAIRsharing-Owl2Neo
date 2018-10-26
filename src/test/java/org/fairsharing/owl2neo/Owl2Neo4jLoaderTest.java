package org.fairsharing.owl2neo;

import org.junit.*;

import org.junit.rules.ExternalResource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Visitor;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class Owl2Neo4jLoaderTest {

    public static final String TEST_GRAPH_DB_PATH = "var/drao-test.db";

    @Rule
    public ResourceFile file = new ResourceFile("/DRAO-inferred.owl");

    private Owl2Neo4jLoader loader;
    private GraphDatabaseService graphDb;

    @Before
    public void setUp() throws Exception {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(TEST_GRAPH_DB_PATH));
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(file.getFile());
        OWLDataFactory factory = manager.getOWLDataFactory();
        loader = new Owl2Neo4jLoader(graphDb, ontology, factory);
    }

    @After
    public void tearDown(){
        graphDb.shutdown();
        try {
            FileUtils.deleteDirectory(new File(TEST_GRAPH_DB_PATH));
        }
        catch (IOException err) {
            System.err.println("Could not delete directory: " + TEST_GRAPH_DB_PATH);
        }
    }

    @Test
    public void loadSynonymsFromOntology() throws Exception {
    }

    @Test
    public void loadOboAlternativeTermFromOntology() throws Exception {
    }

    @Test
    public void loadInSubjectAnnotationProperty() throws Exception {
        loader.loadInSubjectAnnotationProperty();
        Assert.assertNotNull(loader.getInSubject());
    }

    @Test
    public void loadOWLAnnotationPropertyFromOntologyByIriString() throws Exception {
    }

    @Test
    public void getOrCreateOwlThing() throws Exception {
    }

    @Test
    public void importOntology() throws Exception {
        // String graphDbPath = "";
        // GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphDbPath));
        String label = "DOMAIN";
        loader.loadAlternativeTermsFromOntology();
        loader.loadSynonymsFromOntology();
        loader.loadInSubjectAnnotationProperty();
        loader.importOntology(label);
        // Assert.assertTrue(graphDb.getAllNodes().stream().count() > 0);
        // Assert.assertTrue(graphDb.findNodes(Label.label(label)).stream().count() > 0);
    }

    @Test
    public void getOptions() throws Exception {
    }

    @Test
    public void determineLabel() throws Exception {
    }

}