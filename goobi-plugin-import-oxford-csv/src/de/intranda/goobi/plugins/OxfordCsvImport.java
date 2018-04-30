package de.intranda.goobi.plugins;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.goobi.beans.Processproperty;
import org.goobi.production.enums.ImportReturnValue;
import org.goobi.production.enums.ImportType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.importer.DocstructElement;
import org.goobi.production.importer.ImportObject;
import org.goobi.production.importer.Record;
import org.goobi.production.plugin.interfaces.IImportPluginVersion2;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.goobi.production.properties.ImportProperty;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.forms.MassImportForm;
import de.sub.goobi.helper.exceptions.ImportPluginException;
import lombok.extern.log4j.Log4j;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.DocStructType;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.Prefs;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;
import ugh.fileformats.mets.MetsMods;

@PluginImplementation
@Log4j
public class OxfordCsvImport implements IImportPluginVersion2, IPlugin {

    private static final Logger logger = Logger.getLogger(OxfordCsvImport.class);

    private static final String NAME = "oxford_import_csv";
    private String data = "";
    private String csvFolder;
    private String imagesFolder;
    private String targetFolder;
    private String publicationType;
    private String importFolder;
    private Path importFile;
    private Prefs prefs;
    private MassImportForm form;

    public OxfordCsvImport() {
        XMLConfiguration config = ConfigPlugins.getPluginConfig(NAME);
        csvFolder = config.getString("path_csv_files");
        imagesFolder = config.getString("path_image_files");
        targetFolder = config.getString("target_folder");
        publicationType = config.getString("publication_type");
    }

    public String getId() {
        return NAME;
    }

    @Override
    public PluginType getType() {
        return PluginType.Import;
    }

    @Override
    public String getTitle() {
        return NAME;
    }

    public String getDescription() {
        return NAME;
    }

    @Override
    public void setPrefs(Prefs prefs) {
        this.prefs = prefs;
    }

    @Override
    public void setData(Record r) {
        this.data = r.getData();
    }

    @Override
    public Fileformat convertData() throws ImportPluginException {
        return null;
    }

    @Override
    public String getImportFolder() {
        return this.importFolder;
    }

    @Override
    public String getProcessTitle() {
        return null;
    }

    @Override
    public void setImportFolder(String folder) {
        this.importFolder = folder;
    }

    @Override
    public List<Record> splitRecords(String records) {
        return new ArrayList<Record>();
    }

    @Override
    public void setFile(File importFile) {
        this.importFile = importFile.toPath();
    }

    @Override
    public List<String> splitIds(String ids) {
        return null;
    }

    @Override
    public List<ImportType> getImportTypes() {
        List<ImportType> typeList = new ArrayList<ImportType>();
        typeList.add(ImportType.FOLDER);
        return typeList;
    }

    @Override
    public List<ImportProperty> getProperties() {
        return new ArrayList<ImportProperty>();
    }

    @Override
    public List<String> getAllFilenames() {
        List<String> fileNames = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(csvFolder))) {
            for (Path path : directoryStream) {
                if (path.toString().endsWith(".csv")) {
                    fileNames.add(path.getFileName().toString());
                }
            }
        } catch (IOException ex) {
        }
        return fileNames;
    }

    @Override
    public void deleteFiles(List<String> selectedFilenames) {
    }

    @Override
    public List<DocstructElement> getCurrentDocStructs() {
        return null;
    }

    @Override
    public String deleteDocstruct() {
        return null;
    }

    @Override
    public String addDocstruct() {
        return null;
    }

    @Override
    public List<String> getPossibleDocstructs() {
        return null;
    }

    @Override
    public DocstructElement getDocstruct() {
        return null;
    }

    @Override
    public void setDocstruct(DocstructElement dse) {
    }

    @Override
    public void setForm(MassImportForm form) {
        this.form = form;
    }

    @Override
    public List<Record> generateRecordsFromFilenames(List<String> filenames) {
        List<Record> records = new ArrayList<Record>();
        for (String filename : filenames) {

            String item = "";
            try {
                Reader in = new FileReader(new File(csvFolder, filename));
                Iterable<CSVRecord> rows = CSVFormat.RFC4180.withHeader("item", "box", "author", "title", "image").parse(in);
                for (CSVRecord csv : rows) {
                    // create a new record for each new item
                    if (!csv.get("item").equals(item)) {
                        // create new import object as the item is a new one
                        item = csv.get("item");
                        Record rec = new Record();
                        rec.setId(filename + " : " + item);
                        records.add(rec);
                    }

                }
            } catch (IOException e) {
                logger.error("Error while reading the CSV file", e);
            }
        }
        return records;
    }

    @Override
    public List<Record> generateRecordsFromFile() {
        return null;
    }

    @Override
    public List<ImportObject> generateFiles(List<Record> records) {
        List<ImportObject> answer = new ArrayList<ImportObject>();

        // for each generated record (mixture of csv file and item identifier)
        for (Record record : records) {
            String csvFileName = record.getId().substring(0, record.getId().indexOf(" : "));
            String itemName = record.getId().substring(record.getId().indexOf(" : ") + 3);
            ImportObject io = null;
            File targetFolderImages = null;

            // run through whole csv file and create one import object for just this item
            try {
                Reader in = new FileReader(new File(csvFolder, csvFileName));
                Iterable<CSVRecord> rows = CSVFormat.RFC4180.withHeader("item", "box", "author", "title", "image").parse(in);
                for (CSVRecord csv : rows) {

                    if (csv.get("item").equals(itemName)) {

                        // create new import object as this has not been done yet
                        if (io == null) {
                            io = new ImportObject();

                            try {
                                // generate process title
                                String processTitle = csv.get("item");
                                // remove non-ascii characters for the sake of TIFF header limits
                                String regex = ConfigurationHelper.getInstance().getProcessTitleReplacementRegex();
                                processTitle = processTitle.replaceAll(regex, "_");
                                processTitle = processTitle.replaceAll("-", "_");
                                io.setProcessTitle(processTitle);

                                // generate a mets file
                                Fileformat ff = new MetsMods(prefs);
                                DigitalDocument digitalDocument = new DigitalDocument();
                                ff.setDigitalDocument(digitalDocument);
                                DocStructType logicalType = prefs.getDocStrctTypeByName(publicationType);
                                DocStruct logical = digitalDocument.createDocStruct(logicalType);
                                digitalDocument.setLogicalDocStruct(logical);
                                DocStructType physicalType = prefs.getDocStrctTypeByName("BoundBook");
                                DocStruct physical = digitalDocument.createDocStruct(physicalType);
                                digitalDocument.setPhysicalDocStruct(physical);
                                Metadata imagePath = new Metadata(prefs.getMetadataTypeByName("pathimagefiles"));
                                imagePath.setValue("./images/");
                                physical.addMetadata(imagePath);

                                Metadata identifier = new Metadata(prefs.getMetadataTypeByName("CatalogIDDigital"));
                                identifier.setValue(csv.get("item"));
                                logical.addMetadata(identifier);

                                Metadata maintitle = new Metadata(prefs.getMetadataTypeByName("TitleDocMain"));
                                maintitle.setValue((String) csv.get("title"));
                                logical.addMetadata(maintitle);

                                // write mets file into import folder
                                String fileName = getImportFolder() + File.separator + processTitle + ".xml";
                                ff.write(fileName);
                                io.setMetsFilename(fileName);

                                // check if media or master folder to use
                                String tfi = getImportFolder() + File.separator + processTitle + File.separator + "images" + File.separator;
                                if (targetFolder.equals("master")) {
                                    tfi += "master_";
                                }
                                targetFolderImages = new File(tfi + processTitle + "_media");
                                targetFolderImages.mkdirs();

                                // Item as property
                                Processproperty myItem = new Processproperty();
                                myItem.setTitel("Item");
                                myItem.setWert(csv.get("item"));
                                io.getProcessProperties().add(myItem);

                                // Box as property
                                Processproperty myBox = new Processproperty();
                                myBox.setTitel("Box");
                                myBox.setWert(csv.get("box"));
                                io.getProcessProperties().add(myBox);

                                // Author as property
                                Processproperty myAuthor = new Processproperty();
                                myAuthor.setTitel("Author");
                                myAuthor.setWert(csv.get("author"));
                                io.getProcessProperties().add(myAuthor);

                                // Title as property
                                Processproperty myTitle = new Processproperty();
                                myTitle.setTitel("Title");
                                myTitle.setWert(csv.get("title"));
                                io.getProcessProperties().add(myTitle);

                            } catch (WriteException | PreferencesException | MetadataTypeNotAllowedException | TypeNotAllowedForParentException e) {
                                io.setImportReturnValue(ImportReturnValue.WriteError);
                            }
                        }

                        // finally copy all images into temp folder
                        File imageFile = new File(imagesFolder, csv.get("item") + "/" + csv.get("image"));
                        FileUtils.copyFile(imageFile, new File(targetFolderImages, csv.get("image")));
                        log.debug("copied image file from " + imageFile.getAbsolutePath() + " to " + targetFolderImages.getAbsolutePath());
                    }

                }
            } catch (IOException e) {
                logger.error("Error while reading the CSV file and copying images", e);
                io.setImportReturnValue(ImportReturnValue.WriteError);
                io.setErrorMessage("Error while reading the CSV file and copying images: " + e.getMessage());
            }

            // after parsing the csv file add the last import object too
            if (io != null) {
                answer.add(io);
            }
        }
        // end of all selected csv files
        return answer;
    }

    @Override
    public boolean isRunnableAsGoobiScript() {
        return true;
    }

    /**
     * main method just for testing the developments
     * 
     * @param args
     */
    public static void main(String[] args) {
        OxfordCsvImport pci = new OxfordCsvImport();
        pci.setFile(new File("/opt/digiverso/goobi/tmp/03183-P0002 item list 20170616.csv"));
        pci.generateRecordsFromFile();
        System.out.println("finished");
    }

}
