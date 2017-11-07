package org.fairsharing.owl2neo;

import org.apache.commons.cli.*;

public class Utils {

    public static final int OK_STATUS = 0;
    public static final int ERR_STATUS = 1;

    public static CommandLine parseCommandLine(Options options, String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd =  parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(Owl2Neo4jLoader.class.getSimpleName(), options);
            System.exit(ERR_STATUS);
        }
        return cmd;
    }

}
