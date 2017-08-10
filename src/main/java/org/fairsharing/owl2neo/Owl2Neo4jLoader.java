package org.fairsharing.owl2neo;

import openllet.owlapi.OpenlletReasonerFactory;
import org.apache.commons.cli.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import org.semanticweb.owlapi.search.EntitySearcher;
import org.semanticweb.owlapi.util.OWLAPIStreamUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.Map;

public class Owl2Neo4jLoader {

    private static final int OK_STATUS = 0;
    private static final int ERR_STATUS = 1;
    private static final String HASH = "#";
    private static final String GREATER_THAN = ">";
    private static final String IS_A = "isA";

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
                if (classString.contains(HASH)) {
                    classString = classString.substring(classString.indexOf(HASH)+1, classString.indexOf(GREATER_THAN));
                }
                IRI iri = c.getIRI();
                String iriString = iri.getIRIString();
                for (OWLAnnotationProperty property: OWLAPIStreamUtils.asList(c.annotationPropertiesInSignature())) {
                    System.out.println("Annotation property: " + property);
                }
                for (OWLAnnotation annotation: OWLAPIStreamUtils.asList(EntitySearcher.getAnnotations(c, ontology))) {
                    System.out.println("Annotation: " + annotation);
                    OWLAnnotationProperty property = annotation.getProperty();
                    System.out.println(property.toString());
                    OWLLiteral literal = (OWLLiteral) annotation.getValue();
                    System.out.println(literal.getLiteral());
                }
                System.out.println("Current OWL class is: " + classString);
                Node classNode = getOrCreateWithUniqueFactory(classString);
                NodeSet<OWLClass> superClasses = reasoner.getSubClasses(c, true);

                if (superClasses.isEmpty()) {
                    classNode.createRelationshipTo(thingNode, RelationshipType.withName(IS_A));
                }
                else {
                    for (org.semanticweb.owlapi.reasoner.Node<OWLClass> parentOWLNode: superClasses) {
                        OWLClassExpression parent = parentOWLNode.getRepresentativeElement();
                        String parentString = parent.toString();
                        if (parentString.contains(HASH)) {
                            parentString = parentString.substring(classString.indexOf(HASH)+1, classString.indexOf(GREATER_THAN));
                        }
                        Node parentNode = getOrCreateWithUniqueFactory(parentString);
                        classNode.createRelationshipTo(parentNode, RelationshipType.withName(IS_A));
                    }
                }

                for (org.semanticweb.owlapi.reasoner.Node<OWLNamedIndividual> ind : reasoner.getInstances(c, true)) {
                    OWLNamedIndividual individual = ind.getRepresentativeElement();
                    String individualString = individual.toString();
                    System.out.println("Current individual is: " + individualString);
                }
            }
            tx.success();

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
            System.err.println("Exception caught: " + e.getMessage());
            e.printStackTrace();
            System.exit(ERR_STATUS);
        }

    }


}
