package org.fairsharing.owl2neo;

import openllet.owlapi.OpenlletReasonerFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;


import org.semanticweb.owlapi.search.EntitySearcher;
import uk.ac.manchester.cs.jfact.JFactFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Owl2Neo4jLoader {

    private static final String HASH = "#";
    private static final String GREATER_THAN = ">";
    private static final String IS_A = "isA";
    private static final String PART_OF = "partOf";

    public static final String GRAPH_DB_PATH = "var/fairsharing-ont-lite.db";
    // public static final String GRAPH_DB_PATH = "neo4j-community-3.2.3/data/fairsharing-ont.db";
    private static final String OWL_THING = "owl:Thing";

    public static final String OPENLLET = "OPENLLET";

    public static final String PELLET = "PELLET";
    public static final String HERMIT = "HERMIT";
    public static final String JFACT = "JFACT";

    public static final String IN_SUBJECT_VALUE_FAIRSHARING = "FAIRsharing";

    private static final String IN_SUBJECT_IRI = "http://www.geneontology.org/formats/oboInOwl#inSubset";

    private static final String OBO_ALTERNATIVE_TERM_IRI = "http://purl.obolibrary.org/obo/IAO_0000118";
    private static final String OBO_DEFINITION_IRI = "http://purl.obolibrary.org/obo/IAO_0000115";
    private static final String FAIRSHARING_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/DRAO_0000001";
    private static final String RE_3_DATA_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000268";
    private static final String EDAM_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000269";
    private static final String OMIT_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000272";
    private static final String NCIT_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000276";
    private static final String AGRO_VOC_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000278";
    private static final String AGRO_PORTAL_ALTERNATIVE_TERM_IRI = "http://www.fairsharing.org/ontology/SRAO_0000279";
    private static final String PO_ALTERNATIVE_TERM = "http://www.fairsharing.org/ontology/SRAO_0000292";

    private static final String OIO_HAS_EXACT_SYNONYM = "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym";
    private static final String OIO_HAS_RELATED_SYNONYM = "http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym";
    private static final String OIO_HAS_BROAD_SYNONYM = "http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym";

    private GraphDatabaseService graphDb;
    private OWLOntology ontology;
    private OWLDataFactory dataFactory;

    private OWLReasoner reasoner;

    private OWLAnnotationProperty oboAlternativeTerm;
    private OWLAnnotationProperty definition;
    private OWLAnnotationProperty inSubject;
    private Map<String, OWLAnnotationProperty> alternativeTermMap;
    private Map<String, OWLAnnotationProperty> synonymMap;

    @Inject
    public Owl2Neo4jLoader(GraphDatabaseService graphDb, OWLOntology ontology, OWLDataFactory dataFactory) {
        this.graphDb = graphDb;
        this.ontology = ontology;
        this.dataFactory = dataFactory;
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    public void setGraphDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public OWLAnnotationProperty getInSubject() {
        return inSubject;
    }

    public OWLReasoner getReasoner() {
        return OpenlletReasonerFactory.getInstance().createReasoner(ontology);
    }

    public OWLReasoner getReasoner(String reasonerType) {
        switch (reasonerType) {
            case HERMIT:
                return (new ReasonerFactory()).createReasoner(ontology);

            case JFACT:
                return (new JFactFactory()).createReasoner(ontology);

            default:
                return OpenlletReasonerFactory.getInstance().createReasoner(ontology);
        }

    }

    public OWLOntology getOntology() {
        return ontology;
    }

    public void setOntology(OWLOntology ontology) {
        this.ontology = ontology;
    }

    public OWLAnnotationProperty getOboAlternativeTerm() {
        return oboAlternativeTerm;
    }

    public void setOboAlternativeTerm(OWLAnnotationProperty oboAlternativeTerm) {
        this.oboAlternativeTerm = oboAlternativeTerm;
    }

    public void createConstraints() {
        System.out.println("createConstraints() - creating constraints...");
        graphDb.execute("CREATE CONSTRAINT ON (discipline:DISCIPLINE) ASSERT discipline.iri IS UNIQUE;");
        graphDb.execute("CREATE CONSTRAINT ON (domain:DOMAIN) ASSERT domain.iri IS UNIQUE;");
        graphDb.execute("CREATE CONSTRAINT ON (udt:`USER DEFINED TAG`) ASSERT udt.name IS UNIQUE;");
        System.out.println("createConstraints() - constraints created...");
     }

    public void loadSynonymsFromOntology() {
        String[] iris = { OIO_HAS_EXACT_SYNONYM,  OIO_HAS_RELATED_SYNONYM, OIO_HAS_BROAD_SYNONYM };
        synonymMap = new HashMap<String, OWLAnnotationProperty>();
        for (String iri : iris) {
            Optional<OWLAnnotationProperty> optional = ontology.annotationPropertiesInSignature()
                    .filter((OWLAnnotationProperty ap) -> ap.getIRI().getIRIString().equalsIgnoreCase(iri)).findFirst();
            if (optional.isPresent()) {
                System.out.println("loadSynonymsFromOntology() - Synonym " + iri + " is: " + optional.get());
                OWLAnnotationProperty synonym = optional.get();
                String remainderString = synonym.getIRI().getRemainder().get();
                System.out.println("loadSynonymsFromOntology() - Synonym " + remainderString + " is: " + optional.get());
                synonymMap.put(remainderString, synonym);
            }
            else {
                System.out.println("loadSynonymsFromOntology() - Synonym " + iri + " is not found in Ontology " + ontology);
            }
        }

    }

    public void loadAlternativeTermsFromOntology() {
        String[] iris = {FAIRSHARING_ALTERNATIVE_TERM_IRI, OBO_ALTERNATIVE_TERM_IRI, RE_3_DATA_ALTERNATIVE_TERM_IRI,
        EDAM_ALTERNATIVE_TERM_IRI, OMIT_ALTERNATIVE_TERM_IRI, NCIT_ALTERNATIVE_TERM_IRI, AGRO_VOC_ALTERNATIVE_TERM_IRI,
        AGRO_PORTAL_ALTERNATIVE_TERM_IRI, PO_ALTERNATIVE_TERM};
        alternativeTermMap = new HashMap<String, OWLAnnotationProperty>();
        for (String iri : iris) {
            Optional<OWLAnnotationProperty> optional = ontology.annotationPropertiesInSignature()
                    .filter((OWLAnnotationProperty ap) -> ap.getIRI().getIRIString().equalsIgnoreCase(iri)).findFirst();
            if (optional.isPresent()) {
                System.out.println("loadAlternativeTermsFromOntology() - Alternative Term " + iri + " is: " + optional.get());
                OWLAnnotationProperty synonym = optional.get();
                String remainderString = synonym.getIRI().getRemainder().get();
                System.out.println("loadAlternativeTermFromOntology() - Alternative Term " + remainderString + " is: " + optional.get());
                alternativeTermMap.put(remainderString, synonym);
            }
            else {
                System.out.println("loadAlternativeTermFromOntology() - Alternative Term " + iri + " is not found in Ontology " + ontology);
            }
        }
    }

    private OWLAnnotationProperty getAnnotationPropertyFromOntology(String annotationPropertyIri) {
        OWLAnnotationProperty res = null;
        Optional<OWLAnnotationProperty> optional = ontology.annotationPropertiesInSignature()
                .filter((OWLAnnotationProperty ap) -> ap.getIRI().getIRIString().equalsIgnoreCase(annotationPropertyIri)).findFirst();
        if (optional.isPresent()) {
            res = optional.get();
            System.out.println("InSubject property is: " + optional.get());
        }
        else {
            System.out.println("InSubject property not found in Ontology: " + ontology);
        }
        return res;
    }

    public void loadInSubjectAnnotationProperty() {
        inSubject = getAnnotationPropertyFromOntology(IN_SUBJECT_IRI);
    }

    public void loadDefinitionAnnotationProperty() {
        definition = getAnnotationPropertyFromOntology(OBO_DEFINITION_IRI);
    }

    public List<OWLAnnotationProperty> loadOWLAnnotationPropertyFromOntologyByIriString(String iriString) {
        List<OWLAnnotationProperty> owlAnnotationProperties = new ArrayList<OWLAnnotationProperty>();
        Optional<OWLAnnotationProperty> optional = ontology.annotationPropertiesInSignature()
                .filter((OWLAnnotationProperty ap) -> ap.getIRI().getIRIString().equalsIgnoreCase(iriString)).findFirst();
        if (optional.isPresent()) {
            owlAnnotationProperties.add(optional.get());
        }
        return owlAnnotationProperties;
    }

    private Node getOrCreateWithUniqueFactory(String nodeName) {

        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, "index") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("className", properties.get("name"));
            }
        };

        return factory.getOrCreate("name", nodeName);
    }

    private void loadAnnotationProperties() throws Exception {
        final OWLReasoner reasoner = getReasoner();
        if (!reasoner.isConsistent()) throw new Exception("Ontology is inconsistent");

        ontology.annotationPropertiesInSignature().forEach((OWLAnnotationProperty ap) -> {
            System.out.println("Annotation property:" + ap);
        });

    }

    private void loadNodes() throws Exception {
        final OWLReasoner reasoner = getReasoner();
        if (!reasoner.isConsistent()) throw new Exception("Ontology is inconsistent");

        Transaction tx = graphDb.beginTx();

        try {
            getOrCreateWithUniqueFactory(OWL_THING);
            ontology.classesInSignature().forEach((OWLClass c) -> {
                String classString = c.toString(), classLabel = classString;
                if (classString.contains(HASH)) {
                    classString = classString.substring(classString.indexOf(HASH)+1, classString.lastIndexOf(GREATER_THAN));
                }
                IRI iri = c.getIRI();
                String iriString = iri.getIRIString();

                Node classNode = getOrCreateWithUniqueFactory(classString);
                classNode.setProperty("iri", iriString);

                EntitySearcher.getAnnotations(c, ontology, dataFactory.getRDFSLabel()).forEach(annotation -> {
                    System.out.println("Annotation: " + annotation);
                    OWLAnnotationProperty property = annotation.getProperty();
                    System.out.println(property.toString());
                    OWLLiteral literal = (OWLLiteral) annotation.getValue();
                    System.out.println(literal.getLiteral());
                    classNode.setProperty("name", literal.getLiteral());
                });
                System.out.println("Current OWL class is: " + classString);

            });
            tx.success();
        }
        finally {
            tx.close();
        }

    }

    private void loadLinks() throws Exception {
        final OWLReasoner reasoner = getReasoner();
        if (!reasoner.isConsistent()) throw new Exception("Ontology is inconsistent");

        Transaction tx = graphDb.beginTx();

        try {
            ontology.classesInSignature().forEach((OWLClass c) -> {
                Node thingNode = getOrCreateWithUniqueFactory(OWL_THING);
                String classString = c.toString(), classLabel = classString;
                if (classString.contains(HASH)) {
                    classString = classString.substring(classString.indexOf(HASH)+1, classString.lastIndexOf(GREATER_THAN));
                }
                Node classNode = getOrCreateWithUniqueFactory(classString);
                NodeSet<OWLClass> superClasses = reasoner.getSuperClasses(c, true);

                if (superClasses.isEmpty()) {
                    classNode.createRelationshipTo(thingNode, RelationshipType.withName(IS_A));
                }
                else {
                    for (org.semanticweb.owlapi.reasoner.Node<OWLClass> parentOWLNode: superClasses) {
                        OWLClassExpression parent = parentOWLNode.getRepresentativeElement();
                        String parentString = parent.toString();
                        if (parentString.contains(HASH)) {
                            parentString = parentString.substring(parentString.indexOf(HASH)+1, parentString.lastIndexOf(GREATER_THAN));
                        }
                        Node parentNode = getOrCreateWithUniqueFactory(parentString);
                        classNode.createRelationshipTo(parentNode, RelationshipType.withName(PART_OF));
                    }
                }
            });
            tx.success();
        }
        finally {
            tx.close();
        }

    }

    public Node getOrCreateOwlThing() {
        /*
        Transaction tx = graphDb.beginTx();
        try {
            tx = graphDb.beginTx(); */
            Node thingNode = getOrCreateWithUniqueFactory(OWL_THING);
            // tx.success();
            return thingNode;
        /*
        }
        finally {
            tx.close();
        }*/
    }

    public void importOntology(String label) throws Exception {
        final OWLReasoner reasoner = getReasoner();
        if (!reasoner.isConsistent()) {
            throw new Exception("Ontology is inconsistent");
        }

        //load declared OWLAnnotationProperties
        ontology.annotationPropertiesInSignature().forEach((OWLAnnotationProperty ap) -> {
            System.out.println("Annotation property:" + ap);
        });

        Transaction tx = graphDb.beginTx();
        try {
            Node thingNode = getOrCreateOwlThing();

            final AtomicInteger counter = new AtomicInteger();
            long totalCount = ontology.classesInSignature().count();
            System.out.println("Total count is: " + totalCount);
            ontology.classesInSignature().forEach(c -> {
                long start = System.nanoTime();
                loadClassAsNode(reasoner, thingNode, c, label);
                long end = System.nanoTime();
                double duration = end - start / 1000000;
                System.out.println("Duration of current iteration is:" + duration + " ms");
                System.out.println("Done item #" + counter.incrementAndGet());
            });
            tx.success();
        }
        catch (Exception e) {
            System.err.println("Owch, shucks, exception thrown:" + e.getMessage());
            e.printStackTrace();
        }
        finally {
            tx.close();
        }

    }

    private void loadClassAsNode(OWLReasoner reasoner, Node thingNode, OWLClass c, String label) {
        String classString = c.toString(), classLabel = classString;
        if (classString.contains(HASH)) {
            classString = classString.substring(classString.indexOf(HASH)+1, classString.indexOf(GREATER_THAN));
        }
        IRI iri = c.getIRI();
        String iriString = iri.getIRIString();

        // Transaction tx = graphDb.beginTx();
        // try {
            Node classNode = getOrCreateWithUniqueFactory(classString);
            classNode.setProperty("iri", iriString);
            classNode.addLabel(Label.label(label));

            EntitySearcher.getAnnotations(c, ontology, dataFactory.getRDFSLabel()).forEach(annotation -> {
                System.out.println("Annotation: " + annotation);
                OWLAnnotationProperty property = annotation.getProperty();
                System.out.println(property.toString());
                OWLLiteral literal = (OWLLiteral) annotation.getValue();
                System.out.println(literal.getLiteral());
                classNode.setProperty("name", literal.getLiteral());
                classNode.setProperty("displayName", literal.getLiteral());
            });

            List<String> alternativeNames = new ArrayList<String>();
            for (Map.Entry<String, OWLAnnotationProperty> entry : alternativeTermMap.entrySet()) {
                OWLAnnotationProperty alternativeTerm = entry.getValue();
                EntitySearcher.getAnnotations(c, ontology, alternativeTerm).forEach(annotation -> {
                    System.out.println("Annotation alternative term: " + annotation);
                    OWLLiteral literal = (OWLLiteral) annotation.getValue();
                    String propetyIRIString = alternativeTerm.getIRI().getIRIString();
                    switch (propetyIRIString) {
                        case OBO_ALTERNATIVE_TERM_IRI:
                            alternativeNames.add(literal.getLiteral());
                            break;
                        case FAIRSHARING_ALTERNATIVE_TERM_IRI:
                            alternativeNames.add(literal.getLiteral());
                            classNode.setProperty("displayName", literal.getLiteral());
                            break;
                        default:
                            alternativeNames.add(literal.getLiteral());
                    }
                });

            }

            List<String> synonyms = new ArrayList<String>(), exactSynonyms = new ArrayList<String>(),
                relatedSynonyms = new ArrayList<String>(), broadSynonyms = new ArrayList<String>();
            for (Map.Entry<String, OWLAnnotationProperty> entry : synonymMap.entrySet()) {
                EntitySearcher.getAnnotations(c, ontology, entry.getValue()).forEach(annotation -> {
                    System.out.println("Found synonym of type " + entry.getKey() + ": " + annotation);
                    OWLLiteral literal = (OWLLiteral) annotation.getValue();
                    synonyms.add(literal.getLiteral());
                    String propetyIRIString = entry.getValue().getIRI().getIRIString();
                    switch (propetyIRIString) {
                        case OIO_HAS_EXACT_SYNONYM:
                            exactSynonyms.add(literal.getLiteral());
                            break;
                        case OIO_HAS_BROAD_SYNONYM:
                            broadSynonyms.add(literal.getLiteral());
                            break;
                        case OIO_HAS_RELATED_SYNONYM:
                            relatedSynonyms.add(literal.getLiteral());
                            break;
                    }
                });
            }

            if (inSubject != null) {
                EntitySearcher.getAnnotations(c, ontology, inSubject).forEach(annotation -> {
                    String annotationLiteral = ((OWLLiteral) annotation.getValue()).getLiteral();
                    if (annotationLiteral.equalsIgnoreCase(IN_SUBJECT_VALUE_FAIRSHARING)) {
                        classNode.setProperty("isInSubjectFAIRsharing", true);
                    }
                });
                if (classNode.getProperty("isInSubjectFAIRsharing", null) == null) {
                    classNode.setProperty("isInSubjectFAIRsharing", false);
                }
            }

            if (definition != null) {
                Optional<OWLAnnotation> opt = EntitySearcher.getAnnotations(c, ontology, definition).findFirst();
                if (opt.isPresent()) {
                    OWLAnnotation annotation = opt.get();
                    String annotationLiteral = ((OWLLiteral) annotation.getValue()).getLiteral();
                    classNode.setProperty("definition", annotationLiteral);
                }
            }



            String alternativeNamesString = String.join(",", alternativeNames);
            System.out.println("Alternative terms are: " + alternativeNamesString);
            System.out.println("Display name is: " + classNode.getProperty("displayName", null));
            System.out.println("Definition is: " + classNode.getProperty("definition", null));

            // classNode.setProperty("alternativeNames", alternativeNamesString);

            classNode.setProperty("alternativeNames", alternativeNames.toArray(new String[alternativeNames.size()]));
            classNode.setProperty("synonyms", synonyms.toArray(new String[synonyms.size()]));
            classNode.setProperty("exactSynonyms", exactSynonyms.toArray(new String[exactSynonyms.size()]));
            classNode.setProperty("broadSynonyns", broadSynonyms.toArray(new String[broadSynonyms.size()]));
            classNode.setProperty("relatedSynonyms", relatedSynonyms.toArray(new String[relatedSynonyms.size()]));
            System.out.println("Current OWL class is: " + classString);

            NodeSet<OWLClass> superClasses = reasoner.getSuperClasses(c, true);

            if (superClasses.isEmpty()) {
                classNode.createRelationshipTo(thingNode, RelationshipType.withName(IS_A));
            } else {
                for (org.semanticweb.owlapi.reasoner.Node<OWLClass> parentOWLNode : superClasses) {
                    OWLClassExpression parent = parentOWLNode.getRepresentativeElement();
                    String parentString = parent.toString();
                    if (parentString.contains(HASH)) {
                        parentString = parentString.substring(parentString.indexOf(HASH) + 1, parentString.indexOf(GREATER_THAN));
                    }
                    Node parentNode = getOrCreateWithUniqueFactory(parentString);
                    classNode.createRelationshipTo(parentNode, RelationshipType.withName(PART_OF));
                }
            }
        /*
            tx.success();
        }
        finally {
            tx.close();
        }*/
    }

    protected static Options getOptions() {
        Option ontologyPath = new Option("o", "ontology-path", true, "The local location of the OWL file");
        ontologyPath.setRequired(true);
        Options options = new Options();
        options.addOption(ontologyPath);
        Option dbPath = new Option("d", "db-path", true, "The local location of the database");
        dbPath.setRequired(false);
        options.addOption(dbPath);
        return options;
    }

    protected static String determineLabel(String filename) {
        filename = filename.toLowerCase();
        if (filename.contains("disciplines") || filename.contains("srao")) return "DISCIPLINE";
        if (filename.contains("fairsharing") || filename.contains("drao")) return "DOMAIN";
        if (filename.contains("taxon")) return "SPECIES";
        else return "GENERIC";
    }

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = Utils.parseCommandLine(getOptions(), args);
        String graphDbPath = cmd.getOptionValue("d", Owl2Neo4jLoader.GRAPH_DB_PATH);

        try {
            System.out.println("Deleting graph database directory");
            FileUtils.deleteDirectory(new File(graphDbPath));
        }
        catch (IOException e) {
            System.err.println("Exception caught: " + e.getMessage());
        }

        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphDbPath));
        String[] owlFiles = cmd.getOptionValues("o");

        for (String filePath : owlFiles) {
            File file = new File(filePath.trim());
            try {
                OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
                System.out.println("Preparing to load ontology from file:" + file.getAbsolutePath());
                OWLOntology ontology = manager.loadOntologyFromOntologyDocument(file);
                OWLDataFactory factory = manager.getOWLDataFactory();
                System.out.println("Loaded ontology" + ontology);
                Owl2Neo4jLoader loader = new Owl2Neo4jLoader(graphDb, ontology, factory);
                loader.createConstraints();
                String label = determineLabel(filePath);
                System.out.println("Label is: " + label);
                loader.loadAlternativeTermsFromOntology();
                loader.loadSynonymsFromOntology();
                loader.loadInSubjectAnnotationProperty();
                loader.loadDefinitionAnnotationProperty();
                loader.importOntology(label);
            }
            catch (Exception e) {
                System.err.println("Exception caught: " + e.getMessage());
                e.printStackTrace();
                System.exit(Utils.ERR_STATUS);
            }
        }
        graphDb.shutdown();
        System.out.println("Exiting with success...");
        System.exit(Utils.OK_STATUS);

    }



}
