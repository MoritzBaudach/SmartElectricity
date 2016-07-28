package de.farberg.spark.examples.Webserver;

import de.farberg.spark.examples.logic.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uniluebeck.itm.util.logging.Logging;
import spark.Request;
import spark.Response;
import spark.Route;


/**
 * Created by krischke on 28.07.2016.
 */
public class Webserver {

    static int webServerPort = 8080;

    static {
        Logging.setLoggingDefaults();
    }

    public static void main(String[] args) {
        // Obtain an instance of a logger for this class
        Logger log = LoggerFactory.getLogger(Webserver.class);

        // Start a web server
        setupWebServer(webServerPort);
        log.info("Web server started on port " + webServerPort);
        log.info("Open http://localhost:" + webServerPort + " and/or http://localhost:" + webServerPort + "/hello");

        // Do your stuff here

    }

    public static void setupWebServer(int webServerPort) {
        // Set the web server's port
        spark.Spark.port(webServerPort);

        // Serve static files from src/main/resources/webroot
        spark.Spark.staticFiles.location("/webroot");

        // Return "Hello World" at URL /hello
        //spark.Spark.get("/tsv", (req, res) -> "Hello World");

        spark.Spark.get("/tsv", new Route() {
            @Override
            public Object handle(Request request, Response response) throws Exception {
                String householdNumber = request.queryParams("household");
                System.out.println(householdNumber);
                //TODO: Generate TSV based on household id;
                return "Hello World";
            }
        });

        spark.Spark.get("/household/:household", new Route() {
            @Override
            public Object handle(Request request, Response response) throws Exception {
                int householdNumber = Integer.parseInt(request.params(":household"));
                System.out.println(householdNumber);

                /*
                TSV tsv;
                //TODO: Generate TSV based on household id;
                return tsv;
                */
                return Controller.getInstance().requestDevicesAndConsumption(householdNumber);
            }
        });

        // Wait for server to be initialized
        spark.Spark.awaitInitialization();
    }
}
