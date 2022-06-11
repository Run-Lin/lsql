package com.r.spark.repl.sql;

import java.util.*;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class OptionsProcessor {
    protected static final Log l4j = LogFactory.getLog(OptionsProcessor.class.getName());
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    Map<String, String> hiveVariables = new HashMap<String, String>();
    List<String> initFiles = new ArrayList<>();
    private boolean setIsSilent;
    public static final String HIVERCFILE = ".hiverc";

    @SuppressWarnings("static-access")
    public OptionsProcessor() {

        // -database database
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("databasename")
                .withLongOpt("database")
                .withDescription("Specify the database to use")
                .create());

        // -e 'quoted-query-string'
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("quoted-query-string")
                .withDescription("SQL from command line")
                .create('e'));

        // -f <query-file>
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("filename")
                .withDescription("SQL from files")
                .create('f'));

        // -i <init-query-file>
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("filename")
                .withDescription("Initialization SQL file")
                .create('i'));

        // -hiveconf x=y
        options.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("property=value")
                .withLongOpt("hiveconf")
                .withDescription("Use value for given property")
                .create());

        // Substitution option -d, --define
        options.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("key=value")
                .withLongOpt("define")
                .withDescription("Variable subsitution to apply to hive commands. e.g. -d A=B or --define A=B")
                .create('d'));

        // Substitution option --hivevar
        options.addOption(OptionBuilder
                .withValueSeparator()
                .hasArgs(2)
                .withArgName("key=value")
                .withLongOpt("hivevar")
                .withDescription("Variable subsitution to apply to hive commands. e.g. --hivevar A=B")
                .create());

        // [-S|--silent]
        options.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

        // [-v|--verbose]
        options.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the console)"));

        // [-H|--help]
        options.addOption(new Option("H", "help", false, "Print help information"));

    }

    public boolean process_stage1(String[] argv) {
        try {
            commandLine = new GnuParser().parse(options, argv);
            Properties confProps = commandLine.getOptionProperties("hiveconf");
            for (String propKey : confProps.stringPropertyNames()) {
                System.setProperty(propKey, confProps.getProperty(propKey));
            }

            Properties hiveVars = commandLine.getOptionProperties("define");
            for (String propKey : hiveVars.stringPropertyNames()) {
                hiveVariables.put(propKey, hiveVars.getProperty(propKey));
            }

            Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
            for (String propKey : hiveVars2.stringPropertyNames()) {
                hiveVariables.put(propKey, hiveVars2.getProperty(propKey));
            }
            String[] initFileArray = commandLine.getOptionValues('i');
            if(initFileArray != null && initFileArray.length != 0) {
                this.initFiles = Arrays.asList(initFileArray);
            }
            this.setIsSilent = commandLine.hasOption('S');
            String execString = commandLine.getOptionValue("e");
            String fileName = commandLine.getOptionValue("f");
            if (execString != null && fileName != null) {
                System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
                printUsage();
                return false;
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsage();
            return false;
        }
        return true;
    }

    public String process_stage2(String[] argv) {
        StringBuilder sb = new StringBuilder();
        if (commandLine.hasOption("hiveconf")) {
            Properties confProps = commandLine.getOptionProperties("hiveconf");
            for (String propKey : confProps.stringPropertyNames()) {
                sb.append("set ").append(propKey).append("=").append(confProps.getProperty(propKey)).append(";");
            }
        }
        // use args to set default database
        if (commandLine.hasOption("database")) {
            String database = commandLine.getOptionValue("database");
            if(database != null && database.length() > 0) {
                sb.append("use ").append(database).append(";");
            }
        }
        // prevent NPE;
        if (sb.length() == 0) {
            return "";
        }
        return sb.toString();
    }

    private void printUsage() {
        new HelpFormatter().printHelp("hive", options);
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public boolean getIsSilent() {
        return setIsSilent;
    }

    public void setIsSilent(boolean isSlient) {
        this.setIsSilent = isSlient;
    }

    public List<String> getInitFiles() {
        return this.initFiles;
    }
}