client-example
==============

This package contains an example of how a client can be created to work with trays of plant pots in PODD and other systems.

To use this code you should copy this code out into a separate repository to customise it for your system.

PODD Client Workflow
====================

PODD at CSIRO HRPPC encodes the information about the project, experiment, genus, species, and pot (and tray for TrayScan) using a barcode. An example is:

Eg., Project#2014-0001_Experiment#0002_Genus.species_Tray#00023_Pot#00002

These can be decompiled using a Regular Expression to get each of the parts.:

https://github.com/podd/podd-examples/blob/master/client-example/src/main/java/com/github/podd/example/ExampleSpreadsheetConstants.java#L85

The initial steps are to cache the URIs locally for the Artifact/Project/Experiment that you are referring to. The Artifact URI is to locate the whole artifact, the Project URI which is for the top object in the Artifact, and the Experiment URI defines which part of the Project you are working with.

Those parts should be created manually when you decide to take on a clients Project and any subsequent Experiments that are followups for the initial Experiment would add more Experiment instances to the Project as child objects. The main reason for creating these manually is that you (or someone else at your site) will need to assign permissions/roles to users for each Project and this is easiest done using the HTML interface although you can automate it with HTTP calls if you prefer that method.

Once you have those you need to find the Genotype URI which is shared across all of the Experiments in the Project. The Genotypes are attached to the Project overall so that they can easily be referenced by each Experiment. Although this process can be automated, as you will discover in the ExamplePoddClient, you may want to manually insert each of the relevant Genotypes into PODD using the HTML interface to ensure that they are all correct and complete.

Then you need to find the Tray/Pot URIs, which must only be inserted once for each Experiment, and you must reference the existing URI if they are already in PODD.

Each of those queries are done using an HTTP query by POSTing a SPARQL query to the PODD SPARQL endpoint ( http://localhost:8080/podd/sparql ) (The doSPARQL method is defined in the base PoddClient interface and implemented in the RestletPoddClientImpl superclass if you are wondering from reading the ExamplePoddClient code where it comes from).

Then once you have that information you can start to store Event objects representing each of the Lemnatec events. Alexandre Mairin has merged in his Events ontology which defines a few of the events that are going to be relevant to him, which from discussions at the Retreat, I think will also be relevant to you.

For example, you could query the Lemnatec SQL database for all Irrigation/Watering events. Inserting these events into PODD would link the Pots that were irrigated to their Genotypes, based on the Genotype you assigned when you inserted the Pot into PODD earlier, and track the amount of water that was inserted into each of them at each point in time. As the experiment progresses you can then run SPARQL queries on PODD to track the watering for each Pot, possibly something like the following SPARQL query (the URIs may not be correct but the basic idea is there):

    CONSTRUCT { ?pot a poddScience:Pot . ?event a misteaEvent:Irrigation . ?event misteaEvent:occurs_on ?pot . ?event misteaEvent:starts_on ?date . }
             WHERE { ?pot a poddScience:Pot . ?event a misteaEvent:Irrigation . ?event misteaEvent:occurs_on ?pot . ?event misteaEvent:starts_on ?date . }

For imaging, you could store an Event such as an "Imagery"/"Acquisition" event which could then be queried using something like the following SPARQL query:

    CONSTRUCT { ?pot a poddScience:Pot . ?event a misteaEvent:Acquisition . ?event misteaEvent:occurs_on ?pot . ?event misteaEvent:starts_on ?date . ?event poddBase:hasDataReference ?dataReference . ?dataReference a poddBase:DataReference . ?dataReference ?dataRefProperty ?dataRefValue . }
             WHERE { ?pot a poddScience:Pot . ?event a misteaEvent:Acquisition . ?event misteaEvent:occurs_on ?pot . ?event misteaEvent:starts_on ?date . ?event poddBase:hasDataReference ?dataReference . ?dataReference a poddBase:DataReference . ?dataReference ?dataRefProperty ?dataRefValue . }

The properties in the dataRefProperty will tell you where to locate the image file on a file system. The access to the file could be done with PODD, but I haven't yet implemented the downloadDataReference API as we haven't got that far to needing it here yet. In principle it will just be a small extension to each of the PoddDataRepostiory implementations to stream a file through to an OutputStream if the client requests a download, so I am not expecting it to take long once it gets to the top of the priority list.
