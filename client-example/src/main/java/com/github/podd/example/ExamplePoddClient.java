/**
 * PODD is an OWL ontology database used for scientific project management
 * 
 * Copyright (C) 2009-2013 The University Of Queensland
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.podd.example;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;

import javax.imageio.ImageIO;

import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.SecurityUtils;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPFileTransfer;
import net.schmizz.sshj.userauth.keyprovider.FileKeyProvider;
import net.schmizz.sshj.userauth.keyprovider.PKCS8KeyFile;
import net.schmizz.sshj.userauth.password.PasswordFinder;
import net.schmizz.sshj.xfer.FileSystemFile;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.queryrender.RenderUtils;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;

import au.com.bytecode.opencsv.CSVReader;

import com.github.podd.client.api.PoddClientException;
import com.github.podd.client.impl.restlet.RestletPoddClientImpl;
import com.github.podd.utils.InferredOWLOntologyID;
import com.github.podd.utils.OntologyUtils;
import com.github.podd.utils.PODD;
import com.github.podd.utils.PoddDigestUtils;
import com.github.podd.utils.PoddDigestUtils.Algorithm;

/**
 * Provides operations helping to create and update projects in PODD.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ExamplePoddClient extends RestletPoddClientImpl
{
    /**
     * Default configuration for SSHJ, to avoid recreating it for each client.
     */
    private static final DefaultConfig DEFAULT_CONFIG = new DefaultConfig();
    
    public ExamplePoddClient()
    {
        super();
    }
    
    public ExamplePoddClient(final String poddServerUrl)
    {
        super(poddServerUrl);
    }
    
    private void checkTrayScanServerDetails()
    {
        throw new UnsupportedOperationException("TODO: Implement checkTrayScanServerDetails");
    }
    
    /**
     * Verify that the Java {@link ImageIO} methods can be used to read the PNG.
     * 
     * @param rgbImage
     * @param debugPngOutputFile
     * @throws IOException
     * @throws PoddClientException
     */
    private void debugPng(final Path rgbImage, final Path debugPngOutputFile) throws IOException, PoddClientException
    {
        Files.createFile(debugPngOutputFile);
        try (final InputStream input = Files.newInputStream(rgbImage);
                final OutputStream output = Files.newOutputStream(debugPngOutputFile);)
        {
            final BufferedImage bufferedImage = ImageIO.read(input);
            final boolean write = ImageIO.write(bufferedImage, "png", output);
            if(!write)
            {
                throw new PoddClientException("Could not find PNG writer to test the image correctness");
            }
        }
    }
    
    /**
     * Generates the RDF triples necessary for the given TrayScan parameters and adds the details to
     * the relevant model in the upload queue.
     * 
     * @param projectUriMap
     *            A map of relevant project URIs and their artifact identifiers using their
     *            standardised labels.
     * @param experimentUriMap
     *            A map of relevant experiment URIs and their project URIs using their standardised
     *            labels.
     * @param trayUriMap
     *            A map of relevant tray URIs and their experiment URIs using their standardised
     *            labels.
     * @param potUriMap
     *            A map of relevant pot URIs and their tray URIs using their standardised labels.
     * @param uploadQueue
     *            The upload queue containing all of the models to be uploaded.
     * @param projectYear
     *            The TrayScan parameter detailing the project year for the next tray.
     * @param projectNumber
     *            The TrayScan parameter detailing the project number for the next tray.
     * @param experimentNumber
     *            The TrayScan parameter detailing the experiment number for the next tray.
     * @param plantName
     *            The name of this plant
     * @param plantNotes
     *            Specific notes about this plant
     * @param species
     *            The species for the current line.
     * @param genus
     *            The genus for the current line.
     * @throws PoddClientException
     *             If there is a PODD Client exception.
     * @throws GraphUtilException
     *             If there was an illformed graph.
     */
    public void generateTrayRDF(final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> experimentUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> trayUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> potUriMap,
            final ConcurrentMap<URI, ConcurrentMap<URI, Model>> materialUriMap,
            final ConcurrentMap<URI, ConcurrentMap<URI, Model>> genotypeUriMap,
            final ConcurrentMap<InferredOWLOntologyID, Model> uploadQueue, final ExampleCSVLine nextLine)
        throws PoddClientException, GraphUtilException
    {
        Objects.requireNonNull(nextLine, "Line was null");
        Objects.requireNonNull(nextLine.projectID, "ProjectID in line was null");
        
        final Map<URI, InferredOWLOntologyID> projectDetails =
                this.getProjectDetails(projectUriMap, nextLine.projectID);
        
        final URI nextProjectUri = projectDetails.keySet().iterator().next();
        final InferredOWLOntologyID nextProjectID = projectDetails.get(nextProjectUri);
        
        this.log.debug("Found PODD Project name to URI mapping: {} {}", nextLine.projectID, projectDetails);
        
        final Map<URI, URI> experimentDetails = this.getExperimentDetails(nextLine.experimentID);
        
        final URI nextExperimentUri = experimentDetails.keySet().iterator().next();
        
        // Create or find an existing model for the necessary modifications to this
        // project/artifact
        Model nextResult = new LinkedHashModel();
        final Model putIfAbsent = uploadQueue.putIfAbsent(nextProjectID, nextResult);
        if(putIfAbsent != null)
        {
            nextResult = putIfAbsent;
        }
        
        final URI nextTrayUri = this.getTrayUri(trayUriMap, nextLine.trayID, nextProjectID, nextExperimentUri);
        
        // Check whether plantId already has an assigned URI
        final URI nextPotUri = this.getPotUri(potUriMap, nextLine.plantID, nextProjectID, nextTrayUri);
        
        // Check whether genus/specieis/plantName already has an assigned URI (and automatically
        // assign a temporary URI if it does not)
        final URI nextGenotypeUri =
                this.getGenotypeUri(genotypeUriMap, nextLine.genus, nextLine.species, nextLine.plantName,
                        nextLine.plantLineNumber, nextLine.control, nextProjectID, nextProjectUri);
        
        // // Check whether the material for the given genotype for the given pot already has an
        // assigned URI (and automatically
        // // assign a temporary URI if it does not)
        final URI nextMaterialUri =
                this.getMaterialUri(materialUriMap, nextGenotypeUri, nextProjectID, nextPotUri, nextLine.potNumber,
                        nextLine.plantLineNumber, nextLine.control);
        
        // Add new poddScience:Container for tray
        nextResult.add(nextTrayUri, RDF.TYPE, PODD.PODD_SCIENCE_TRAY);
        // Link tray to experiment
        nextResult.add(nextExperimentUri, PODD.PODD_SCIENCE_HAS_TRAY, nextTrayUri);
        // TrayID => Add poddScience:hasBarcode to tray
        nextResult.add(nextTrayUri, PODD.PODD_SCIENCE_HAS_BARCODE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.trayID));
        // TrayNotes => Add rdfs:label to tray
        nextResult.add(nextTrayUri, RDFS.LABEL, RestletPoddClientImpl.vf.createLiteral(nextLine.trayNotes));
        // TrayTypeName => Add poddScience:hasTrayType to tray
        nextResult.add(nextTrayUri, PODD.PODD_SCIENCE_HAS_TRAY_TYPE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.trayTypeName));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_TRAY_NUMBER,
                RestletPoddClientImpl.vf.createLiteral(nextLine.trayNumber, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_TRAY_ROW_NUMBER,
                RestletPoddClientImpl.vf.createLiteral(nextLine.trayRowNumber, XMLSchema.STRING));
        
        // Add new poddScience:Container for pot
        nextResult.add(nextPotUri, RDF.TYPE, PODD.PODD_SCIENCE_POT);
        // Link pot to tray
        nextResult.add(nextTrayUri, PODD.PODD_SCIENCE_HAS_POT, nextPotUri);
        
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_MATERIAL, nextMaterialUri);
        
        // PlantID => Add poddScience:hasBarcode to pot
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_BARCODE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.plantID));
        
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_NUMBER,
                RestletPoddClientImpl.vf.createLiteral(nextLine.potNumber, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_TYPE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.potType, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_COLUMN_NUMBER_OVERALL,
                RestletPoddClientImpl.vf.createLiteral(nextLine.columnNumber, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_COLUMN_NUMBER_REPLICATE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.columnNumberRep, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_COLUMN_NUMBER_TRAY,
                RestletPoddClientImpl.vf.createLiteral(nextLine.columnNumberTray, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_COLUMN_LETTER_TRAY,
                RestletPoddClientImpl.vf.createLiteral(nextLine.columnLetter));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_POSITION_TRAY,
                RestletPoddClientImpl.vf.createLiteral(nextLine.position));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_NUMBER_TRAY,
                RestletPoddClientImpl.vf.createLiteral(nextLine.potNumberTray, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_NUMBER_REPLICATE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.potReplicateNumber, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_REPLICATE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.replicateNumber, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_ROW_NUMBER_REPLICATE,
                RestletPoddClientImpl.vf.createLiteral(nextLine.rowNumberRep, XMLSchema.STRING));
        nextResult.add(nextPotUri, PODD.PODD_SCIENCE_HAS_POT_ROW_NUMBER_TRAY,
                RestletPoddClientImpl.vf.createLiteral(nextLine.rowNumberTray, XMLSchema.STRING));
        
        if(nextGenotypeUri.stringValue().startsWith(RestletPoddClientImpl.TEMP_UUID_PREFIX))
        {
            // Add all of the statements for the genotype to the update to make sure that temporary
            // descriptions are added
            nextResult.addAll(genotypeUriMap.get(nextProjectUri).get(nextGenotypeUri));
        }
        
        if(nextMaterialUri.stringValue().startsWith(RestletPoddClientImpl.TEMP_UUID_PREFIX))
        {
            // Add all of the statements for the genotype to the update to make sure that temporary
            // descriptions are added
            nextResult.addAll(materialUriMap.get(nextPotUri).get(nextMaterialUri));
        }
        
        String potLabel;
        // PlantNotes => Add rdfs:label to pot
        if(nextLine.plantNotes == null || nextLine.plantNotes.isEmpty())
        {
            potLabel = "Pot " + nextLine.plantName;
        }
        else
        {
            potLabel = "Pot " + nextLine.plantName + " : " + nextLine.plantNotes;
        }
        nextResult.add(nextPotUri, RDFS.LABEL, RestletPoddClientImpl.vf.createLiteral(potLabel));
        
    }
    
    /**
     * @param experimentUriMap
     * @param baseExperimentName
     * @return
     * @throws PoddClientException
     */
    private Map<URI, URI> getExperimentDetails(final String baseExperimentName) throws PoddClientException
    {
        throw new UnsupportedOperationException("TODO: Implement getExperimentDetails");
    }
    
    /**
     * Gets a genotype URI matching the given genus, species, and plantName (line) from the given
     * cache, creating a new entry if necessary and giving it a temporary URI.
     * 
     * @param genotypeUriMap
     * @param genus
     * @param species
     * @param plantName
     * @param control
     * @param nextProjectID
     * @param nextProjectUri
     * @return
     */
    private URI getGenotypeUri(final ConcurrentMap<URI, ConcurrentMap<URI, Model>> genotypeUriMap, final String genus,
            final String species, final String plantName, final String plantLineNumber, final String control,
            final InferredOWLOntologyID nextProjectID, final URI nextProjectUri)
    {
        URI nextGenotypeURI = null;
        if(genotypeUriMap.containsKey(nextProjectUri))
        {
            final ConcurrentMap<URI, Model> nextProjectGenotypeMap = genotypeUriMap.get(nextProjectUri);
            
            for(final URI existingGenotypeURI : nextProjectGenotypeMap.keySet())
            {
                final Model nextModel = nextProjectGenotypeMap.get(existingGenotypeURI);
                
                if(nextModel.contains(existingGenotypeURI, PODD.PODD_SCIENCE_HAS_GENUS,
                        RestletPoddClientImpl.vf.createLiteral(genus)))
                {
                    if(nextModel.contains(existingGenotypeURI, PODD.PODD_SCIENCE_HAS_SPECIES,
                            RestletPoddClientImpl.vf.createLiteral(species)))
                    {
                        if(nextModel.contains(existingGenotypeURI, PODD.PODD_SCIENCE_HAS_LINE,
                                RestletPoddClientImpl.vf.createLiteral(plantName)))
                        {
                            nextGenotypeURI = existingGenotypeURI;
                            break;
                        }
                        else
                        {
                            this.log.debug(
                                    "Did not find any genotypes with the given genus and species and line in this project: {} {} {} {}",
                                    nextProjectUri, genus, species, plantName);
                        }
                    }
                    else
                    {
                        this.log.debug(
                                "Did not find any genotypes with the given genus and species in this project: {} {} {}",
                                nextProjectUri, genus, species);
                    }
                }
                else
                {
                    this.log.debug("Did not find any genotypes with the given genus in this project: {} {}",
                            nextProjectUri, genus);
                }
            }
        }
        
        // If no genotype was found, then create a new description and assign it a temporary URI
        if(nextGenotypeURI == null)
        {
            this.log.debug(
                    "Could not find an existing genotype for description provided, assigning a temporary URI: {} {} {} {}",
                    nextProjectID, genus, species, plantName);
            
            nextGenotypeURI =
                    RestletPoddClientImpl.vf.createURI(RestletPoddClientImpl.TEMP_UUID_PREFIX + "genotype:"
                            + plantLineNumber + ":" + UUID.randomUUID().toString());
            
            final Model newModel = new LinkedHashModel();
            newModel.add(nextProjectUri, PODD.PODD_SCIENCE_HAS_GENOTYPE, nextGenotypeURI);
            newModel.add(nextGenotypeURI, RDF.TYPE, PODD.PODD_SCIENCE_GENOTYPE);
            newModel.add(nextGenotypeURI, RDFS.LABEL,
                    RestletPoddClientImpl.vf.createLiteral(genus + " " + species + " (" + plantName + ")"));
            newModel.add(
                    nextGenotypeURI,
                    RDFS.COMMENT,
                    RestletPoddClientImpl.vf.createLiteral("Plant line in : ", genus + " " + species + " named, "
                            + plantName + " : labelled as number " + plantLineNumber));
            newModel.add(nextGenotypeURI, PODD.PODD_SCIENCE_HAS_GENUS, RestletPoddClientImpl.vf.createLiteral(genus));
            newModel.add(nextGenotypeURI, PODD.PODD_SCIENCE_HAS_SPECIES,
                    RestletPoddClientImpl.vf.createLiteral(species));
            newModel.add(nextGenotypeURI, PODD.PODD_SCIENCE_HAS_LINE, RestletPoddClientImpl.vf.createLiteral(plantName));
            newModel.add(nextGenotypeURI, PODD.PODD_SCIENCE_HAS_LINE_NUMBER,
                    RestletPoddClientImpl.vf.createLiteral(plantLineNumber));
            
            ConcurrentMap<URI, Model> nextGenotypeUriMap = new ConcurrentHashMap<>();
            final ConcurrentMap<URI, Model> putIfAbsent =
                    genotypeUriMap.putIfAbsent(nextProjectUri, nextGenotypeUriMap);
            if(putIfAbsent != null)
            {
                nextGenotypeUriMap = putIfAbsent;
            }
            final Model putIfAbsent2 = nextGenotypeUriMap.putIfAbsent(nextGenotypeURI, newModel);
            if(putIfAbsent2 != null)
            {
                this.log.error("ERROR: Generated two temporary Genotype URIs that were identical! : {} {}",
                        nextProjectUri, nextGenotypeURI);
            }
        }
        return nextGenotypeURI;
        
    }
    
    public List<String> getImageFormatNames()
    {
        final String[] formats = ImageIO.getWriterFormatNames();
        // Need to de-duplicate format names that come as both upper and lower case
        final HashSet<String> formatSet = new HashSet<String>();
        for(final String s : formats)
        {
            formatSet.add(s.toLowerCase());
        }
        final List<String> result = new ArrayList<>(formatSet);
        Collections.sort(result);
        return result;
    }
    
    /**
     * Gets a material URI matching the given pot and genotype URIs, creating a new entry if
     * necessary and giving it a temporary URI.
     * 
     * @param materialUriMap
     * @param nextProjectID
     * @param nextPotUri
     * @return
     */
    private URI getMaterialUri(final ConcurrentMap<URI, ConcurrentMap<URI, Model>> materialUriMap,
            final URI nextGenotypeUri, final InferredOWLOntologyID nextProjectID, final URI nextPotUri,
            final String potNumber, final String lineNumber, final String control)
    {
        URI nextMaterialURI = null;
        if(materialUriMap.containsKey(nextPotUri))
        {
            final ConcurrentMap<URI, Model> nextPotMaterialMap = materialUriMap.get(nextPotUri);
            
            for(final URI existingMaterialURI : nextPotMaterialMap.keySet())
            {
                final Model nextModel = nextPotMaterialMap.get(existingMaterialURI);
                
                if(nextModel.contains(existingMaterialURI, PODD.PODD_SCIENCE_REFERS_TO_GENOTYPE, nextGenotypeUri))
                {
                    nextMaterialURI = existingMaterialURI;
                }
                else
                {
                    this.log.debug("Did not find any materials with the given genotype in this pot: {} {}", nextPotUri,
                            nextGenotypeUri);
                }
            }
        }
        
        // If no material was found, then create a new description and assign it a temporary URI
        if(nextMaterialURI == null)
        {
            this.log.debug(
                    "Could not find an existing material for description provided, assigning a temporary URI: {} {} {}",
                    nextProjectID, nextPotUri, nextGenotypeUri);
            
            nextMaterialURI =
                    RestletPoddClientImpl.vf.createURI(RestletPoddClientImpl.TEMP_UUID_PREFIX + "material:"
                            + UUID.randomUUID().toString());
            
            final Model newModel = new LinkedHashModel();
            newModel.add(nextPotUri, PODD.PODD_SCIENCE_HAS_MATERIAL, nextMaterialURI);
            newModel.add(nextMaterialURI, RDF.TYPE, PODD.PODD_SCIENCE_MATERIAL);
            
            newModel.add(
                    nextMaterialURI,
                    RDFS.LABEL,
                    RestletPoddClientImpl.vf.createLiteral("Material for pot " + potNumber + " containing line "
                            + lineNumber));
            newModel.add(nextMaterialURI, PODD.PODD_SCIENCE_REFERS_TO_GENOTYPE, nextGenotypeUri);
            if(control.equalsIgnoreCase("Yes"))
            {
                newModel.add(nextMaterialURI, PODD.PODD_SCIENCE_HAS_CONTROL, PODD.PODD_SCIENCE_HAS_CONTROL_YES);
            }
            else if(control.equalsIgnoreCase("No"))
            {
                newModel.add(nextMaterialURI, PODD.PODD_SCIENCE_HAS_CONTROL, PODD.PODD_SCIENCE_HAS_CONTROL_NO);
            }
            else
            {
                this.log.warn("Did not recognise control label: {} (should be Yes or No", control);
                newModel.add(nextMaterialURI, PODD.PODD_SCIENCE_HAS_CONTROL, PODD.PODD_SCIENCE_HAS_CONTROL_UNKNOWN);
            }
            
            ConcurrentMap<URI, Model> nextGenotypeUriMap = new ConcurrentHashMap<>();
            final ConcurrentMap<URI, Model> putIfAbsent = materialUriMap.putIfAbsent(nextPotUri, nextGenotypeUriMap);
            if(putIfAbsent != null)
            {
                nextGenotypeUriMap = putIfAbsent;
            }
            final Model putIfAbsent2 = nextGenotypeUriMap.putIfAbsent(nextMaterialURI, newModel);
            if(putIfAbsent2 != null)
            {
                this.log.error("ERROR: Generated two temporary Material URIs that were identical! : {} {}", nextPotUri,
                        nextMaterialURI);
            }
        }
        return nextMaterialURI;
        
    }
    
    /**
     * Get a connection to the TrayScanDB database.
     * 
     * @return A connection to the SQL database for TrayScan
     * @throws SQLException
     */
    public Connection getMySQLConnection() throws SQLException
    {
        throw new UnsupportedOperationException("TODO: Implement getMySQLConnection");
        // return DriverManager.getConnection("jdbc:mysql://" + this.getTrayScanHost() + "/" +
        // this.getTrayScanDbName(),
        // this.getTrayScanUsername(), this.getTrayScanPassword());
    }
    
    /**
     * @param potUriMap
     * @param plantId
     * @param nextProjectID
     * @param nextTrayURI
     * @return
     * @throws PoddClientException
     * @throws GraphUtilException
     */
    private URI getPotUri(final ConcurrentMap<String, ConcurrentMap<URI, URI>> potUriMap, final String plantId,
            final InferredOWLOntologyID nextProjectID, final URI nextTrayURI) throws PoddClientException,
        GraphUtilException
    {
        URI nextPotURI;
        if(potUriMap.containsKey(plantId))
        {
            nextPotURI = potUriMap.get(plantId).keySet().iterator().next();
        }
        else
        {
            final Model plantIdSparqlResults =
                    this.doSPARQL(String.format(ExampleSpreadsheetConstants.TEMPLATE_SPARQL_BY_TYPE_LABEL_STRSTARTS,
                            RenderUtils.escape(plantId), RenderUtils.getSPARQLQueryString(PODD.PODD_SCIENCE_POT)),
                            nextProjectID);
            
            if(plantIdSparqlResults.isEmpty())
            {
                this.log.debug(
                        "Could not find an existing container for pot barcode, assigning a temporary URI: {} {}",
                        plantId, nextProjectID);
                
                nextPotURI =
                        RestletPoddClientImpl.vf.createURI(RestletPoddClientImpl.TEMP_UUID_PREFIX + "pot:"
                                + UUID.randomUUID().toString());
            }
            else
            {
                nextPotURI = GraphUtil.getUniqueSubjectURI(plantIdSparqlResults, RDF.TYPE, PODD.PODD_SCIENCE_POT);
            }
            
            ConcurrentMap<URI, URI> nextPotUriMap = new ConcurrentHashMap<>();
            final ConcurrentMap<URI, URI> putIfAbsent2 = potUriMap.putIfAbsent(plantId, nextPotUriMap);
            if(putIfAbsent2 != null)
            {
                nextPotUriMap = putIfAbsent2;
            }
            nextPotUriMap.put(nextPotURI, nextTrayURI);
        }
        return nextPotURI;
    }
    
    /**
     * @param projectUriMap
     * @param baseProjectName
     * @return
     * @throws PoddClientException
     */
    private Map<URI, InferredOWLOntologyID> getProjectDetails(
            final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap,
            final String baseProjectName) throws PoddClientException
    {
        if(!projectUriMap.containsKey(baseProjectName))
        {
            this.log.error("Did not find an existing project for a line in the CSV file: {}", baseProjectName);
            
            // TODO: Create a new project?
            // return;
            
            throw new PoddClientException("Did not find an existing project for a line in the CSV file: "
                    + baseProjectName);
        }
        
        return projectUriMap.get(baseProjectName);
    }
    
    private Model getTopObject(final InferredOWLOntologyID nextArtifact, final Model artifactDetails)
        throws PoddClientException
    {
        return artifactDetails.filter(
                artifactDetails.filter(nextArtifact.getOntologyIRI().toOpenRDFURI(), PODD.PODD_BASE_HAS_TOP_OBJECT,
                        null).objectURI(), null, null);
    }
    
    /**
     * @param trayUriMap
     * @param trayId
     * @param nextProjectID
     * @param nextExperimentUri
     * @return
     * @throws PoddClientException
     * @throws GraphUtilException
     */
    private URI getTrayUri(final ConcurrentMap<String, ConcurrentMap<URI, URI>> trayUriMap, final String trayId,
            final InferredOWLOntologyID nextProjectID, final URI nextExperimentUri) throws PoddClientException,
        GraphUtilException
    {
        // Check whether trayId already has an assigned URI
        URI nextTrayURI;
        if(trayUriMap.containsKey(trayId))
        {
            nextTrayURI = trayUriMap.get(trayId).keySet().iterator().next();
        }
        else
        {
            final Model trayIdSparqlResults =
                    this.doSPARQL(String.format(ExampleSpreadsheetConstants.TEMPLATE_SPARQL_BY_TYPE_LABEL_STRSTARTS,
                            RenderUtils.escape(trayId), RenderUtils.getSPARQLQueryString(PODD.PODD_SCIENCE_TRAY)),
                            nextProjectID);
            
            if(trayIdSparqlResults.isEmpty())
            {
                this.log.debug(
                        "Could not find an existing container for tray barcode, assigning a temporary URI: {} {}",
                        trayId, nextProjectID);
                
                nextTrayURI =
                        RestletPoddClientImpl.vf.createURI(RestletPoddClientImpl.TEMP_UUID_PREFIX + "tray:"
                                + UUID.randomUUID().toString());
            }
            else
            {
                nextTrayURI = GraphUtil.getUniqueSubjectURI(trayIdSparqlResults, RDF.TYPE, PODD.PODD_SCIENCE_TRAY);
            }
            
            ConcurrentMap<URI, URI> nextTrayUriMap = new ConcurrentHashMap<>();
            final ConcurrentMap<URI, URI> putIfAbsent2 = trayUriMap.putIfAbsent(trayId, nextTrayUriMap);
            if(putIfAbsent2 != null)
            {
                nextTrayUriMap = putIfAbsent2;
            }
            nextTrayUriMap.put(nextTrayURI, nextExperimentUri);
        }
        return nextTrayURI;
    }
    
    /**
     * Inserts the given line into the TrayScan database.
     * 
     * @param nextLine
     * @throws SQLException
     * @throws PoddClientException
     */
    private void insertTrayScanToMySQL(final ExampleCSVLine nextLine) throws SQLException, PoddClientException
    {
        this.checkTrayScanServerDetails();
        
        try (final Connection connection = this.getMySQLConnection();)
        {
            connection.setAutoCommit(false);
            
            final String trayQueryString = "SELECT * FROM tab_tray WHERE tray_textId = ?;";
            try (final PreparedStatement prepareStatement =
                    connection.prepareStatement(trayQueryString, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);)
            {
                prepareStatement.setString(1, nextLine.trayID);
                try (final ResultSet resultSet = prepareStatement.executeQuery())
                {
                    // If the tray did not exist, then store it
                    if(!resultSet.next())
                    {
                        final ResultSetMetaData metaData = resultSet.getMetaData();
                        try (final CallableStatement cs = connection.prepareCall("{call StoreTray(?, ?, ?)}");)
                        {
                            cs.setString(1, nextLine.trayID);
                            cs.setString(2, nextLine.trayTypeName);
                            cs.setString(3, nextLine.trayNotes);
                            try (final ResultSet updateSet = cs.executeQuery();)
                            {
                                // TODO: Check to make sure call succeeded
                            }
                        }
                    }
                }
            }
            
            final String plantQueryString = "SELECT * FROM tab_plant WHERE plant_textId = ?;";
            try (final PreparedStatement prepareStatement =
                    connection.prepareStatement(plantQueryString, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);)
            {
                prepareStatement.setString(1, nextLine.plantID);
                try (final ResultSet resultSet = prepareStatement.executeQuery())
                {
                    // If the plant did not exist, then store it
                    if(!resultSet.next())
                    {
                        final ResultSetMetaData metaData = resultSet.getMetaData();
                        try (final CallableStatement cs = connection.prepareCall("{call StorePlant(?, ?, ?, ?)}");)
                        {
                            cs.setString(1, nextLine.plantID);
                            cs.setString(2, nextLine.potType);
                            cs.setString(3, nextLine.plantName);
                            cs.setString(4, nextLine.plantNotes);
                            try (final ResultSet updateSet = cs.executeQuery();)
                            {
                                // TODO: Check to make sure call succeeded
                            }
                        }
                        // If the plant did not already exist, also link it to its tray
                        // TODO: Check for current assignments and call this if necessary
                        try (final CallableStatement cs = connection.prepareCall("{call StoreAssign(?, ?, ?)}");)
                        {
                            cs.setString(1, nextLine.trayID);
                            cs.setString(2, nextLine.plantID);
                            cs.setString(3, nextLine.position);
                            try (final ResultSet updateSet = cs.executeQuery();)
                            {
                                // TODO: Check to make sure call succeeded
                            }
                        }
                        
                    }
                }
            }
            
            connection.commit();
        }
    }
    
    /**
     * Parses an LST file generated for the randomisation process and returns a CSV file that can be
     * imported into Excel.
     * 
     * @param in
     * @throws IOException
     * @throws PoddClientException
     */
    public void parseLstFile(final InputStream in, final Writer output) throws IOException, PoddClientException
    {
        List<String> headers = null;
        String parsedString;
        try (final InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);)
        {
            parsedString = IOUtils.toString(reader);
        }
        
        final String[] lines = parsedString.split("\r?\n");
        
        final List<List<String>> splitLines = new ArrayList<>();
        
        for(final String nextLineFull : lines)
        {
            splitLines.add(Arrays.asList(nextLineFull.trim().split("\\s+")));
        }
        
        for(final List<String> nextLine : splitLines)
        {
            if(headers == null)
            {
                // header line is mandatory in LST file
                headers = nextLine;
                try
                {
                    this.verifyLstHeaders(headers);
                }
                catch(final IllegalArgumentException e)
                {
                    this.log.error("Could not verify headers for LST file: {}", e.getMessage());
                    throw new PoddClientException("Could not verify headers for LST file", e);
                }
                
                for(int i = 0; i < headers.size(); i++)
                {
                    if(i > 0)
                    {
                        output.write(", ");
                    }
                    output.write(headers.get(i));
                }
                output.write("\r\n");
            }
            else
            {
                if(nextLine.size() != headers.size())
                {
                    this.log.error("Line and header sizes were different: {} {}", headers, nextLine);
                }
                
                for(int i = 0; i < headers.size(); i++)
                {
                    if(i > 0)
                    {
                        output.write(", ");
                    }
                    output.write(nextLine.get(i));
                }
                output.write("\r\n");
            }
        }
        
        if(headers == null)
        {
            this.log.error("Document did not contain a valid header line");
        }
    }
    
    private void populateExperimentUriMap(
            final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> experimentUriMap) throws PoddClientException
    {
        for(final String nextProjectName : projectUriMap.keySet())
        {
            final ConcurrentMap<URI, InferredOWLOntologyID> nextProjectNameMapping = projectUriMap.get(nextProjectName);
            for(final URI projectUri : nextProjectNameMapping.keySet())
            {
                final InferredOWLOntologyID artifactId = nextProjectNameMapping.get(projectUri);
                final Model nextSparqlResults =
                        this.doSPARQL(
                                String.format(ExampleSpreadsheetConstants.TEMPLATE_SPARQL_BY_TYPE,
                                        RenderUtils.getSPARQLQueryString(PODD.PODD_SCIENCE_EXPERIMENT)), artifactId);
                
                if(nextSparqlResults.isEmpty())
                {
                    this.log.info("Could not find any existing experiments for project: {} {}", nextProjectName,
                            projectUri);
                }
                
                for(final Resource nextExperiment : nextSparqlResults.filter(null, RDF.TYPE,
                        PODD.PODD_SCIENCE_EXPERIMENT).subjects())
                {
                    if(!(nextExperiment instanceof URI))
                    {
                        this.log.error("Found experiment that was not assigned a URI: {} artifact={}", nextExperiment,
                                artifactId);
                    }
                    else
                    {
                        final Model label = nextSparqlResults.filter(nextExperiment, RDFS.LABEL, null);
                        
                        // DebugUtils.printContents(label);
                        
                        if(label.isEmpty())
                        {
                            this.log.error("Experiment did not have a label: {} {}", artifactId, nextExperiment);
                        }
                        else
                        {
                            for(final Value nextLabel : label.objects())
                            {
                                if(!(nextLabel instanceof Literal))
                                {
                                    this.log.error("Project had a non-literal label: {} {} {}", artifactId,
                                            nextExperiment, nextLabel);
                                }
                                else
                                {
                                    String nextLabelString = nextLabel.stringValue();
                                    
                                    // take off any descriptions and leave the
                                    // project number behind
                                    nextLabelString = nextLabelString.split(" ")[0];
                                    
                                    final Matcher matcher =
                                            ExampleSpreadsheetConstants.REGEX_EXPERIMENT.matcher(nextLabelString);
                                    
                                    if(!matcher.matches())
                                    {
                                        this.log.error(
                                                "Found experiment label that did not start with expected format: {}",
                                                nextLabel);
                                    }
                                    else
                                    {
                                        this.log.debug(
                                                "Found experiment label with the expected format: '{}' original=<{}>",
                                                nextLabelString, nextLabel);
                                        
                                        final int nextProjectYear = Integer.parseInt(matcher.group(1));
                                        final int nextProjectNumber = Integer.parseInt(matcher.group(2));
                                        final int nextExperimentNumber = Integer.parseInt(matcher.group(3));
                                        
                                        nextLabelString =
                                                String.format(ExampleSpreadsheetConstants.TEMPLATE_EXPERIMENT,
                                                        nextProjectYear, nextProjectNumber, nextExperimentNumber);
                                        
                                        this.log.debug("Reformatted experiment label to: '{}' original=<{}>",
                                                nextLabelString, nextLabel);
                                        
                                        ConcurrentMap<URI, URI> labelMap = new ConcurrentHashMap<>();
                                        final ConcurrentMap<URI, URI> putIfAbsent =
                                                experimentUriMap.putIfAbsent(nextLabelString, labelMap);
                                        if(putIfAbsent != null)
                                        {
                                            this.log.error(
                                                    "Found duplicate experiment name, inconsistent results may follow: {} {} {}",
                                                    artifactId, nextExperiment, nextLabel);
                                            // Overwrite our reference with the one that already
                                            // existed
                                            labelMap = putIfAbsent;
                                        }
                                        final URI existingProject =
                                                labelMap.putIfAbsent((URI)nextExperiment, projectUri);
                                        // Check for the case where project name maps to different
                                        // artifacts
                                        if(existingProject != null && !existingProject.equals(projectUri))
                                        {
                                            this.log.error(
                                                    "Found duplicate experiment name across different projects, inconsistent results may follow: {} {} {} {}",
                                                    artifactId, existingProject, projectUri, nextLabel);
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                }
            }
        }
    }
    
    private void populateGenotypeUriMap(
            final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap,
            final ConcurrentMap<URI, ConcurrentMap<URI, Model>> genotypeUriMap) throws PoddClientException
    {
        for(final String nextProjectName : projectUriMap.keySet())
        {
            final ConcurrentMap<URI, InferredOWLOntologyID> nextProjectNameMapping = projectUriMap.get(nextProjectName);
            for(final URI projectUri : nextProjectNameMapping.keySet())
            {
                final InferredOWLOntologyID artifactId = nextProjectNameMapping.get(projectUri);
                final Model nextSparqlResults =
                        this.doSPARQL(String.format(ExampleSpreadsheetConstants.TEMPLATE_SPARQL_BY_TYPE_ALL_PROPERTIES,
                                RenderUtils.getSPARQLQueryString(PODD.PODD_SCIENCE_GENOTYPE)), artifactId);
                if(nextSparqlResults.isEmpty())
                {
                    this.log.debug("Could not find any existing genotypes for project: {} {}", nextProjectName,
                            projectUri);
                }
                
                for(final Resource nextGenotype : nextSparqlResults.filter(null, RDF.TYPE, PODD.PODD_SCIENCE_GENOTYPE)
                        .subjects())
                {
                    if(!(nextGenotype instanceof URI))
                    {
                        this.log.error("Found genotype that was not assigned a URI: {} artifact={}", nextGenotype,
                                artifactId);
                    }
                    else
                    {
                        ConcurrentMap<URI, Model> nextGenotypeMap = new ConcurrentHashMap<>();
                        final ConcurrentMap<URI, Model> putIfAbsent = genotypeUriMap.put(projectUri, nextGenotypeMap);
                        if(putIfAbsent != null)
                        {
                            nextGenotypeMap = putIfAbsent;
                        }
                        final Model putIfAbsent2 = nextGenotypeMap.putIfAbsent((URI)nextGenotype, nextSparqlResults);
                        if(putIfAbsent2 != null)
                        {
                            this.log.info("Found existing description for genotype URI within the same project: {} {}",
                                    projectUri, nextGenotype);
                        }
                    }
                }
            }
        }
    }
    
    private void populateProjectUriMap(final Model currentUnpublishedArtifacts,
            final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap)
        throws PoddClientException
    {
        for(final InferredOWLOntologyID nextArtifact : OntologyUtils.modelToOntologyIDs(currentUnpublishedArtifacts,
                true, false))
        {
            final Model nextTopObject = this.getTopObject(nextArtifact, currentUnpublishedArtifacts);
            
            // DebugUtils.printContents(nextTopObject);
            
            final Model types = nextTopObject.filter(null, RDF.TYPE, PODD.PODD_SCIENCE_PROJECT);
            if(types.isEmpty())
            {
                // We only map project based artifacts, others are ignored with
                // log messages here
                this.log.info("Found a non-project based artifact, ignoring as it isn't relevant here: {}",
                        nextArtifact);
            }
            else if(types.subjects().size() > 1)
            {
                // We only map single-project based artifacts, others are
                // ignored with log messages here
                this.log.error("Found multiple projects for an artifact: {} {}", nextArtifact, types.subjects());
            }
            else
            {
                final Resource project = types.subjects().iterator().next();
                
                if(!(project instanceof URI))
                {
                    // We only map URI references, as blank nodes which are
                    // allowable, cannot be reserialised to update the artifact,
                    // and should not exist
                    this.log.error("Found non-URI project reference for an artifact: {} {}", nextArtifact,
                            types.subjects());
                }
                else
                {
                    final Model label = nextTopObject.filter(project, RDFS.LABEL, null);
                    
                    // DebugUtils.printContents(label);
                    
                    if(label.isEmpty())
                    {
                        this.log.error("Project did not have a label: {} {}", nextArtifact, project);
                    }
                    else
                    {
                        for(final Value nextLabel : label.objects())
                        {
                            if(!(nextLabel instanceof Literal))
                            {
                                this.log.error("Project had a non-literal label: {} {} {}", nextArtifact, project,
                                        nextLabel);
                            }
                            else
                            {
                                String nextLabelString = nextLabel.stringValue();
                                
                                // take off any descriptions and leave the
                                // project number behind
                                nextLabelString = nextLabelString.split(" ")[0];
                                
                                final Matcher matcher =
                                        ExampleSpreadsheetConstants.REGEX_PROJECT.matcher(nextLabelString);
                                
                                if(!matcher.matches())
                                {
                                    this.log.error("Found project label that did not start with expected format: {}",
                                            nextLabel);
                                }
                                else
                                {
                                    this.log.debug("Found project label with the expected format: '{}' original=<{}>",
                                            nextLabelString, nextLabel);
                                    
                                    final int nextProjectYear = Integer.parseInt(matcher.group(1));
                                    final int nextProjectNumber = Integer.parseInt(matcher.group(2));
                                    
                                    nextLabelString =
                                            String.format(ExampleSpreadsheetConstants.TEMPLATE_PROJECT,
                                                    nextProjectYear, nextProjectNumber);
                                    
                                    this.log.debug("Reformatted project label to: '{}' original=<{}>", nextLabelString,
                                            nextLabel);
                                    
                                    ConcurrentMap<URI, InferredOWLOntologyID> labelMap = new ConcurrentHashMap<>();
                                    final ConcurrentMap<URI, InferredOWLOntologyID> putIfAbsent =
                                            projectUriMap.putIfAbsent(nextLabelString, labelMap);
                                    if(putIfAbsent != null)
                                    {
                                        this.log.error(
                                                "Found duplicate project name, inconsistent results may follow: {} {} {}",
                                                nextArtifact, project, nextLabel);
                                        // Overwrite our reference with the one that
                                        // already existed
                                        labelMap = putIfAbsent;
                                    }
                                    final InferredOWLOntologyID existingArtifact =
                                            labelMap.putIfAbsent((URI)project, nextArtifact);
                                    // Check for the case where project name maps to
                                    // different artifacts
                                    if(existingArtifact != null && !existingArtifact.equals(nextArtifact))
                                    {
                                        this.log.error(
                                                "Found duplicate project name across different projects, inconsistent results may follow: {} {} {} {}",
                                                nextArtifact, existingArtifact, project, nextLabel);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Parses the mapping of line numbers to the line names used to identify lines in the
     * randomisation process.
     * 
     * @param in
     *            An {@link InputStream} containing the CSV file with the mapping of line numbers to
     *            line names
     * @return A map from line numbers to line names.
     * @throws IOException
     *             If there is an {@link IOException}.
     * @throws PoddClientException
     *             If there is a problem communicating with the PODD server.
     */
    public ConcurrentMap<String, String> processLineNameMappingList(final InputStream in) throws IOException,
        PoddClientException
    {
        // -----------------------------------------------------------------------------------------
        // Now process the CSV file line by line using the caches to reduce multiple queries to the
        // server where possible
        // -----------------------------------------------------------------------------------------
        
        List<String> headers = null;
        final ConcurrentMap<String, String> result = new ConcurrentHashMap<>();
        // Supressing try-with-resources warning generated erroneously by Eclipse:
        // https://bugs.eclipse.org/bugs/show_bug.cgi?id=371614
        try (@SuppressWarnings("resource")
        final InputStreamReader inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
                final CSVReader reader = new CSVReader(inputStreamReader);)
        {
            String[] nextLine;
            while((nextLine = reader.readNext()) != null)
            {
                if(headers == null)
                {
                    // header line is mandatory in PODD CSV
                    headers = Arrays.asList(nextLine);
                    try
                    {
                        if(headers.size() != 2)
                        {
                            throw new IllegalArgumentException("Did not find required number of headers");
                        }
                        
                        if(!headers.get(0).equals(ExampleLineMappingConstants.RAND_LINE_NUMBER))
                        {
                            throw new IllegalArgumentException("Missing "
                                    + ExampleLineMappingConstants.RAND_LINE_NUMBER + " header");
                        }
                        
                        if(!headers.get(1).equals(ExampleLineMappingConstants.RAND_CLIENT_LINE_NAME))
                        {
                            throw new IllegalArgumentException("Missing "
                                    + ExampleLineMappingConstants.RAND_CLIENT_LINE_NAME + " header");
                        }
                    }
                    catch(final IllegalArgumentException e)
                    {
                        this.log.error("Could not verify headers for line name mappings file: {}", e.getMessage());
                        throw new PoddClientException("Could not verify headers for line name mappings file", e);
                    }
                }
                else
                {
                    if(nextLine.length != headers.size())
                    {
                        this.log.error("Line and header sizes were different: {} {}", headers, nextLine);
                    }
                    
                    final String putIfAbsent = result.putIfAbsent(nextLine[0], nextLine[1]);
                    if(putIfAbsent != null)
                    {
                        this.log.error(
                                "Found multiple mappings for line name and number: linenumber={} duplicate={} original={}",
                                nextLine[0], nextLine[1], putIfAbsent);
                    }
                }
            }
        }
        
        if(headers == null)
        {
            this.log.error("Document did not contain a valid header line");
        }
        
        if(result.isEmpty())
        {
            this.log.error("Document did not contain any valid rows");
        }
        
        return result;
    }
    
    /**
     * Process a single line from the input file, using the given headers as the definitions for the
     * line.
     * 
     * @param headers
     *            The list of headers
     * @param nextLine
     *            A list of values which can be matched against the list of headers
     * @param projectUriMap
     *            A map from the normalised project names to their URIs and the overall artifact
     *            identifiers.
     * @param experimentUriMap
     *            A map from normalised experiment names to their URIs and the projects that they
     *            are located in.
     * @param trayUriMap
     *            A map from normalised tray names (barcodes) to their URIs and the experiments that
     *            they are located in.
     * @param potUriMap
     *            A map from normalised pot names (barcodes) to their URIs and the experiments that
     *            they are located in.
     * @param uploadQueue
     *            A map from artifact identifiers to Model objects containing all of the necessary
     *            changes to the artifact.
     * 
     * @throws PoddClientException
     *             If there was a problem communicating with PODD or the line was not valid.
     * @throws SQLException
     *             If there is an issue with the MySQL connection to TrayScanDB
     */
    private void processTrayScanLine(final List<String> headers, final List<String> nextLine,
            final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> experimentUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> trayUriMap,
            final ConcurrentMap<String, ConcurrentMap<URI, URI>> potUriMap,
            final ConcurrentMap<URI, ConcurrentMap<URI, Model>> materialUriMap,
            final ConcurrentMap<URI, ConcurrentMap<URI, Model>> genotypeUriMap,
            final ConcurrentMap<InferredOWLOntologyID, Model> uploadQueue) throws PoddClientException,
        OpenRDFException, SQLException
    {
        this.log.info("About to process line: {}", nextLine);
        final ExampleCSVLine result = new ExampleCSVLine();
        
        for(int i = 0; i < headers.size(); i++)
        {
            final String nextHeader = headers.get(i);
            final String nextField = nextLine.get(i);
            
            if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_YEAR))
            {
                result.year = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PROJECT_NUMBER))
            {
                result.projectNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PROJECT_ID))
            {
                result.projectID = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_EXPERIMENT_NUMBER))
            {
                result.experimentNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_EXPERIMENT_ID))
            {
                result.experimentID = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_GENUS))
            {
                result.genus = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_SPECIES))
            {
                result.species = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_POT_NUMBER))
            {
                result.potNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_TRAY_NUMBER))
            {
                result.trayNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_POT_NUMBER_TRAY))
            {
                result.potNumberTray = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_COLUMN_NUMBER_TRAY))
            {
                result.columnNumberTray = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_COLUMN_LETTER))
            {
                result.columnLetter = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_ROW_NUMBER_TRAY))
            {
                result.rowNumberTray = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_ROW_NUMBER_REP))
            {
                result.rowNumberRep = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_COLUMN_NUMBER_REP))
            {
                result.columnNumberRep = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_COLUMN_NUMBER))
            {
                result.columnNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_TRAY_ID))
            {
                result.trayID = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_TRAY_NOTES))
            {
                result.trayNotes = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_TRAY_ROW_NUMBER))
            {
                result.trayRowNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_TRAY_TYPE_NAME))
            {
                result.trayTypeName = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_POSITION))
            {
                result.position = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PLANT_ID))
            {
                result.plantID = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PLANT_LINE_NUMBER))
            {
                result.plantLineNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PLANT_NAME))
            {
                result.plantName = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_PLANT_NOTES))
            {
                result.plantNotes = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_POT_TYPE))
            {
                result.potType = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_CONTROL))
            {
                result.control = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_REPLICATE_NUMBER))
            {
                result.replicateNumber = nextField;
            }
            else if(nextHeader.trim().equals(ExampleSpreadsheetConstants.CLIENT_POT_REPLICATE_NUMBER))
            {
                result.potReplicateNumber = nextField;
            }
            else
            {
                this.log.error("Found unrecognised header: {} {}", nextHeader, nextField);
                throw new PoddClientException("TODO: Handle unrecognised header: " + nextHeader);
            }
        }
        
        this.generateTrayRDF(projectUriMap, experimentUriMap, trayUriMap, potUriMap, materialUriMap, genotypeUriMap,
                uploadQueue, result);
        
        // Push the line into MySQL
        this.insertTrayScanToMySQL(result);
    }
    
    /**
     * Parses the given TrayScan project/experiment/tray/pot list and inserts the items into PODD
     * where they do not exist.
     * 
     * TODO: Should this process create new projects where they do not already exist? Ideally they
     * should be created and roles assigned before this process, but could be fine to do that in
     * here
     */
    public ConcurrentMap<InferredOWLOntologyID, Model> processTrayScanList(final InputStream in) throws IOException,
        PoddClientException, OpenRDFException, SQLException
    {
        // Keep a queue so that we only need to update each project once for
        // this operation to succeed
        final ConcurrentMap<InferredOWLOntologyID, Model> uploadQueue = new ConcurrentHashMap<>();
        
        // Map starting at project name strings and ending with both the URI of
        // the project and the artifact
        final ConcurrentMap<String, ConcurrentMap<URI, InferredOWLOntologyID>> projectUriMap =
                new ConcurrentHashMap<>();
        
        // Map starting at experiment name strings and ending with a mapping from the URI of
        // the experiment to the URI of the project that contains the experiment
        // TODO: This could be converted to not be prefilled in future, but currently it contains
        // all experiments in all unpublished projects in PODD that are accessible to the current
        // user
        final ConcurrentMap<String, ConcurrentMap<URI, URI>> experimentUriMap = new ConcurrentHashMap<>();
        
        // Material mappings, starting at the URI of the experiment and mapping to the URI of the
        // material and the RDF Model containing the statements describing this material
        final ConcurrentMap<URI, ConcurrentMap<URI, Model>> materialUriMap = new ConcurrentHashMap<>();
        
        // Genotype mappings, starting at the URI of the project and mapping to the URI of the
        // genotype and the RDF Model containing the statements describing this genotype
        final ConcurrentMap<URI, ConcurrentMap<URI, Model>> genotypeUriMap = new ConcurrentHashMap<>();
        
        // Cache for tray name mappings, starting at tray barcodes and ending with a mapping from
        // the URI of the tray to the URI of the experiment that contains the tray
        // NOTE: This is not prefilled, as it is populated on demand during processing of lines to
        // only contain the necessary elements
        final ConcurrentMap<String, ConcurrentMap<URI, URI>> trayUriMap = new ConcurrentHashMap<>();
        
        // Cache for pot name mappings, starting at pot barcodes and ending with a mapping from
        // the URI of the pot to the URI of the tray that contains the pot
        // NOTE: This is not prefilled, as it is populated on demand during processing of lines to
        // only contain the necessary elements
        final ConcurrentMap<String, ConcurrentMap<URI, URI>> potUriMap = new ConcurrentHashMap<>();
        
        // -----------------------------------------------------------------------------------------
        // Now cache URIs for projects, experiments, and genotypes for all unpublished projects that
        // the current user can access
        // -----------------------------------------------------------------------------------------
        
        // Only select the unpublished artifacts, as we cannot edit published artifacts
        final Model currentUnpublishedArtifacts = this.listArtifacts(false, true);
        
        // Map known project names to their URIs, as the URIs are needed to
        // create statements internally
        this.populateProjectUriMap(currentUnpublishedArtifacts, projectUriMap);
        
        this.populateExperimentUriMap(projectUriMap, experimentUriMap);
        
        this.populateGenotypeUriMap(projectUriMap, genotypeUriMap);
        
        // -----------------------------------------------------------------------------------------
        // Now process the CSV file line by line using the caches to reduce multiple queries to the
        // server where possible
        // -----------------------------------------------------------------------------------------
        
        List<String> headers = null;
        // Supressing try-with-resources warning generated erroneously by Eclipse:
        // https://bugs.eclipse.org/bugs/show_bug.cgi?id=371614
        try (@SuppressWarnings("resource")
        final InputStreamReader inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
                final CSVReader reader = new CSVReader(inputStreamReader);)
        {
            String[] nextLine;
            while((nextLine = reader.readNext()) != null)
            {
                if(headers == null)
                {
                    // header line is mandatory in PODD CSV
                    headers = Arrays.asList(nextLine);
                    try
                    {
                        this.verifyTrayScanListHeaders(headers);
                    }
                    catch(final IllegalArgumentException e)
                    {
                        this.log.error("Could not verify headers for project list: {}", e.getMessage());
                        throw new PoddClientException("Could not verify headers for project list", e);
                    }
                }
                else
                {
                    if(nextLine.length != headers.size())
                    {
                        this.log.error("Line and header sizes were different: {} {}", headers, nextLine);
                    }
                    
                    // Process the next line and add it to the upload queue
                    this.processTrayScanLine(headers, Arrays.asList(nextLine), projectUriMap, experimentUriMap,
                            trayUriMap, potUriMap, materialUriMap, genotypeUriMap, uploadQueue);
                }
            }
        }
        
        if(headers == null)
        {
            this.log.error("Document did not contain a valid header line");
        }
        
        if(uploadQueue.isEmpty())
        {
            this.log.error("Document did not contain any valid rows");
        }
        
        return uploadQueue;
    }
    
    public Map<Path, String> uploadToStorage(final List<Path> bagsToUpload, final String sshServerFingerprint,
            final String sshHost, final int portNo, final String username, final Path pathToPublicKey,
            final Path localRootPath, final Path remoteRootPath, final PasswordFinder keyExtractor)
        throws PoddClientException, NoSuchAlgorithmException, IOException
    {
        final Map<Path, String> results = new ConcurrentHashMap<>();
        
        final ConcurrentMap<Path, ConcurrentMap<PoddDigestUtils.Algorithm, String>> digests =
                PoddDigestUtils.getDigests(bagsToUpload);
        
        try (SSHClient sshClient = new SSHClient(ExamplePoddClient.DEFAULT_CONFIG);)
        {
            sshClient.useCompression();
            sshClient.addHostKeyVerifier(sshServerFingerprint);
            sshClient.connect(sshHost, portNo);
            if(!Files.exists(pathToPublicKey))
            {
                throw new PoddClientException("Could not find public key: " + pathToPublicKey);
            }
            if(!SecurityUtils.isBouncyCastleRegistered())
            {
                throw new PoddClientException("Bouncy castle needed");
            }
            final FileKeyProvider rsa = new PKCS8KeyFile();
            rsa.init(pathToPublicKey.toFile(), keyExtractor);
            sshClient.authPublickey(username, rsa);
            // Session session = sshClient.startSession();
            try (SFTPClient sftp = sshClient.newSFTPClient();)
            {
                for(final Path nextBag : bagsToUpload)
                {
                    // Check to make sure that the bag was under the local root path
                    final Path localPath = nextBag.toAbsolutePath();
                    if(!localPath.startsWith(localRootPath))
                    {
                        this.log.error("Local bag path was not a direct descendant of the local root path: {} {} {}",
                                localRootPath, nextBag, localPath);
                        throw new PoddClientException(
                                "Local bag path was not a direct descendant of the local root path: " + localPath + " "
                                        + localRootPath);
                    }
                    
                    // Take the local root path out to get the subpath to use on the remote
                    final Path remoteSubPath =
                            localPath.subpath(localRootPath.getNameCount(), nextBag.getNameCount() - 1);
                    
                    this.log.info("Remote sub path: {}", remoteSubPath);
                    
                    final Path remoteDirPath = remoteRootPath.resolve(remoteSubPath);
                    this.log.info("Remote dir path: {}", remoteDirPath);
                    
                    final Path remoteBagPath = remoteDirPath.resolve(nextBag.getFileName());
                    
                    this.log.info("Remote bag path: {}", remoteBagPath);
                    
                    boolean fileFound = false;
                    boolean sizeCorrect = false;
                    try
                    {
                        // check details of a remote bag
                        final FileAttributes attribs = sftp.lstat(remoteBagPath.toAbsolutePath().toString());
                        final long localSize = Files.size(nextBag);
                        final long remoteSize = attribs.getSize();
                        
                        if(localSize <= 0)
                        {
                            this.log.error("Local bag was empty: {}", nextBag);
                            sizeCorrect = false;
                            fileFound = false;
                        }
                        else if(remoteSize <= 0)
                        {
                            this.log.warn("Remote bag was empty: {} {}", nextBag, attribs);
                            sizeCorrect = false;
                            fileFound = false;
                        }
                        else if(localSize == remoteSize)
                        {
                            this.log.info("Found file on remote already with same size as local: {} {}", nextBag,
                                    remoteBagPath);
                            sizeCorrect = true;
                            fileFound = true;
                        }
                        else
                        {
                            sizeCorrect = false;
                            fileFound = true;
                            // We always assume that a non-zero local file is correct
                            // The bags contain time-stamps that will be modified when they are
                            // regenerated, likely changing the file-size, and hopefully changing
                            // the digest checksums
                            // throw new PoddClientException(
                            // "Could not automatically compare file sizes (need manual intervention to delete one) : "
                            // + nextBag + " " + remoteBagPath + " localSize=" + localSize
                            // + " remoteSize=" + remoteSize);
                        }
                    }
                    catch(final IOException e)
                    {
                        // lstat() throws an IOException if the file does not exist
                        // Ignore
                        sizeCorrect = false;
                        fileFound = false;
                    }
                    
                    final ConcurrentMap<Algorithm, String> bagDigests = digests.get(nextBag);
                    if(bagDigests.isEmpty())
                    {
                        this.log.error("No bag digests were generated for bag: {}", nextBag);
                    }
                    for(final Entry<Algorithm, String> entry : bagDigests.entrySet())
                    {
                        final Path localDigestPath =
                                localPath.resolveSibling(localPath.getFileName() + entry.getKey().getExtension());
                        // Create the local digest file
                        Files.copy(new ReaderInputStream(new StringReader(entry.getValue()), StandardCharsets.UTF_8),
                                localDigestPath);
                        final Path remoteDigestPath =
                                remoteBagPath.resolveSibling(remoteBagPath.getFileName()
                                        + entry.getKey().getExtension());
                        boolean nextDigestFileFound = false;
                        boolean nextDigestCorrect = false;
                        try
                        {
                            final Path tempFile = Files.createTempFile("podd-digest-", entry.getKey().getExtension());
                            final SFTPFileTransfer sftpFileTransfer = new SFTPFileTransfer(sftp.getSFTPEngine());
                            sftpFileTransfer.download(remoteBagPath.toAbsolutePath().toString(), tempFile
                                    .toAbsolutePath().toString());
                            nextDigestFileFound = true;
                            
                            final List<String> allLines = Files.readAllLines(tempFile, StandardCharsets.UTF_8);
                            if(allLines.isEmpty())
                            {
                                nextDigestCorrect = false;
                            }
                            else if(allLines.size() > 1)
                            {
                                nextDigestCorrect = false;
                            }
                            // Check if the digests match exactly
                            else if(allLines.get(0).equals(entry.getValue()))
                            {
                                nextDigestCorrect = true;
                            }
                            else
                            {
                                nextDigestCorrect = false;
                            }
                        }
                        catch(final IOException e)
                        {
                            nextDigestFileFound = false;
                            nextDigestCorrect = false;
                        }
                        if(nextDigestFileFound && nextDigestCorrect)
                        {
                            this.log.info("Not copying digest to remote as it exists and contains the same content as the local digest");
                        }
                        else if(nextDigestFileFound && !nextDigestCorrect)
                        {
                            this.log.error("Found remote digest but content was not correct: {} {}", localDigestPath,
                                    remoteDigestPath);
                            sftp.rm(remoteDigestPath.toString());
                            this.log.info("Copying digest to remote: {}", remoteDigestPath);
                            sftp.put(new FileSystemFile(localDigestPath.toString()), remoteDigestPath.toString());
                        }
                        else if(!nextDigestFileFound)
                        {
                            this.log.info("About to make directories on remote: {}", remoteDirPath);
                            sftp.mkdirs(remoteDirPath.toString());
                            this.log.info("Copying digest to remote: {}", remoteDigestPath);
                            sftp.put(new FileSystemFile(localDigestPath.toString()), remoteDigestPath.toString());
                        }
                    }
                    
                    if(fileFound && sizeCorrect)
                    {
                        this.log.info("Not copying bag to remote as it exists and is the same size as local bag");
                    }
                    else if(fileFound && !sizeCorrect)
                    {
                        this.log.error("Found remote bag but size was not correct: {} {}", nextBag, remoteBagPath);
                        sftp.rm(remoteBagPath.toString());
                        this.log.info("Copying bag to remote: {}", remoteBagPath);
                        sftp.put(new FileSystemFile(localPath.toString()), remoteBagPath.toString());
                    }
                    else if(!fileFound)
                    {
                        this.log.info("About to make directories on remote: {}", remoteDirPath);
                        sftp.mkdirs(remoteDirPath.toString());
                        this.log.info("Copying bag to remote: {}", remoteBagPath);
                        sftp.put(new FileSystemFile(localPath.toString()), remoteBagPath.toString());
                    }
                    
                }
            }
        }
        catch(final IOException e)
        {
            throw new PoddClientException("Could not copy a bag to the remote location", e);
        }
        
        return results;
    }
    
    public ConcurrentMap<InferredOWLOntologyID, InferredOWLOntologyID> uploadToPodd(
            final ConcurrentMap<InferredOWLOntologyID, Model> uploadQueue) throws PoddClientException
    {
        final ConcurrentMap<InferredOWLOntologyID, InferredOWLOntologyID> resultMap = new ConcurrentHashMap<>();
        for(final InferredOWLOntologyID nextUpload : uploadQueue.keySet())
        {
            try
            {
                final StringWriter writer = new StringWriter(4096);
                Rio.write(uploadQueue.get(nextUpload), writer, RDFFormat.RDFJSON);
                final InferredOWLOntologyID newID =
                        this.appendArtifact(nextUpload,
                                new ByteArrayInputStream(writer.toString().getBytes(Charset.forName("UTF-8"))),
                                RDFFormat.RDFJSON);
                
                if(newID == null)
                {
                    this.log.error("Did not find a valid result from append artifact: {}", nextUpload);
                }
                else if(nextUpload.equals(newID))
                {
                    this.log.error("Result from append artifact was not changed, as expected. {} {}", nextUpload, newID);
                }
                else
                {
                    resultMap.putIfAbsent(nextUpload, newID);
                }
            }
            catch(final RDFHandlerException e)
            {
                this.log.error("Found exception generating upload body: ", e);
            }
        }
        return resultMap;
    }
    
    private void verifyLstHeaders(final List<String> headers)
    {
        if(headers == null || headers.size() < ExampleLstConstants.MIN_LST_HEADERS_SIZE)
        {
            this.log.error("Did not find valid headers: {}", headers);
            throw new IllegalArgumentException("Did not find valid headers");
        }
        
        if(!headers.contains(ExampleLstConstants.UNIT))
        {
            throw new IllegalArgumentException("Did not find UNIT header");
        }
        
        if(!headers.contains(ExampleLstConstants.ID))
        {
            throw new IllegalArgumentException("Did not find ID header");
        }
        
        if(!headers.contains(ExampleLstConstants.ENTRY))
        {
            throw new IllegalArgumentException("Did not find ENTRY header");
        }
        
        if(!headers.contains(ExampleLstConstants.ROW))
        {
            throw new IllegalArgumentException("Did not find ROW header");
        }
        
        if(!headers.contains(ExampleLstConstants.RANGE))
        {
            throw new IllegalArgumentException("Did not find RANGE header");
        }
        
        if(!headers.contains(ExampleLstConstants.REP))
        {
            throw new IllegalArgumentException("Did not find REP header");
        }
        
        if(!headers.contains(ExampleLstConstants.TRT))
        {
            throw new IllegalArgumentException("Did not find TRT header");
        }
        
        if(!headers.contains(ExampleLstConstants.B111))
        {
            throw new IllegalArgumentException("Did not find B111 header");
        }
        
        if(!headers.contains(ExampleLstConstants.B121))
        {
            throw new IllegalArgumentException("Did not find B121 header");
        }
        
    }
    
    /**
     * Verifies the list of projects, throwing an IllegalArgumentException if there are unrecognised
     * headers or if any mandatory headers are missing.
     * 
     * @throws IllegalArgumentException
     *             If the headers are not verified correctly.
     */
    public void verifyTrayScanListHeaders(final List<String> headers) throws IllegalArgumentException
    {
        if(headers == null || headers.size() < ExampleSpreadsheetConstants.MIN_TRAYSCAN_HEADERS_SIZE)
        {
            this.log.error("Did not find valid headers: {}", headers);
            throw new IllegalArgumentException("Did not find valid headers");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_TRAY_ID))
        {
            throw new IllegalArgumentException("Did not find tray id header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_TRAY_NOTES))
        {
            throw new IllegalArgumentException("Did not find tray notes header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_TRAY_TYPE_NAME))
        {
            throw new IllegalArgumentException("Did not find tray type name header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_POSITION))
        {
            throw new IllegalArgumentException("Did not find position header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_PLANT_ID))
        {
            throw new IllegalArgumentException("Did not find plant id header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_PLANT_NAME))
        {
            throw new IllegalArgumentException("Did not find plant name header");
        }
        
        if(!headers.contains(ExampleSpreadsheetConstants.CLIENT_PLANT_NOTES))
        {
            throw new IllegalArgumentException("Did not find plant notes header");
        }
    }
    
}