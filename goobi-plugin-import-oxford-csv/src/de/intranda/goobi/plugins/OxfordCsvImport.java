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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
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

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.forms.MassImportForm;
import de.sub.goobi.helper.exceptions.ImportPluginException;
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
public class OxfordCsvImport implements IImportPluginVersion2, IPlugin {

    private static final Logger logger = Logger.getLogger(OxfordCsvImport.class);

    private static final String NAME = "oxford_import_csv";
    private String data = "";
    private String importFolder = "/opt/digiverso/goobi/import/oxford/WTS_New_Examples/";
    private Path importFile;
    private Prefs prefs;
    private MassImportForm form;

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
	public List<Record> generateRecordsFromFilenames(List<String> filenames) {
		List<Record> records = new ArrayList<Record>();
		for (String filename : filenames) {
			Record rec = new Record();
			rec.setData(filename);
			rec.setId(filename);
			records.add(rec);
		}
		return records;
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
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(importFolder,"csvs"))) {
            for (Path path : directoryStream) {
                if (path.toString().endsWith(".csv")){
                	fileNames.add(path.toString());
                }
            }
        } catch (IOException ex) {}
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
    public List<Record> generateRecordsFromFile() {
    	List<Record> records = new ArrayList<Record>();
    	
    	try {
			Reader in = new FileReader(importFile.toFile());
			Iterable<CSVRecord> rows = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
//			Iterable<CSVRecord> rows = CSVFormat.RFC4180.withHeader("barcode", "series_id", "consignment_no", "unit_no", "unit_Item_code", "description").parse(in);
			for (CSVRecord csv : rows) {
				
				HashMap<String, String> map = new HashMap<>();
				map.put("barcode", csv.get("barcode"));
				map.put("series_id", csv.get("series_id"));
				map.put("consignment_no", csv.get("consignment_no"));
				map.put("unit_no", csv.get("unit_no"));
				map.put("unit_Item_code", csv.get("unit_Item_code"));
				map.put("description", csv.get("description"));
				
				Record r = new Record();
				r.setId(map.get("barcode"));
				r.setData(map.get("barcode"));
				r.setObject(map);
				records.add(r);
			}
		} catch (IOException e) {
			logger.error("Error while reading the CSV file", e);
		}
        return records;
    }
    
    /* (non-Javadoc)
     * @see org.goobi.production.plugin.interfaces.IImportPlugin#generateFiles(java.util.List)
     */
    @Override
    public List<ImportObject> generateFiles(List<Record> records) {
    	List<ImportObject> answer = new ArrayList<ImportObject>();

		// for each row in csv file now
    	for (Record record : records) {
	    	ImportObject io = new ImportObject();
	    	try {
	    		Object tempObject = record.getObject();
	            HashMap map = (HashMap) tempObject;
	            
	    		// generate a mets file
	    		Fileformat ff = new MetsMods(prefs);
	            DigitalDocument digitalDocument = new DigitalDocument();
	            ff.setDigitalDocument(digitalDocument);
	            DocStructType logicalType = prefs.getDocStrctTypeByName("Record");
	            DocStruct logical = digitalDocument.createDocStruct(logicalType);
	            digitalDocument.setLogicalDocStruct(logical);
	            DocStructType physicalType = prefs.getDocStrctTypeByName("BoundBook");
	            DocStruct physical = digitalDocument.createDocStruct(physicalType);
	            digitalDocument.setPhysicalDocStruct(physical);
	            Metadata imagePath = new Metadata(prefs.getMetadataTypeByName("pathimagefiles"));
	            imagePath.setValue("./images/");
	            physical.addMetadata(imagePath);
	
	            Metadata identifier = new Metadata(prefs.getMetadataTypeByName("CatalogIDDigital"));
	            identifier.setValue(record.getId());
	            logical.addMetadata(identifier);
	            
	            Metadata maintitle = new Metadata(prefs.getMetadataTypeByName("TitleDocMain"));
	            maintitle.setValue((String) map.get("description"));
	            logical.addMetadata(maintitle);
	
	            // write mets file into import folder
	            String fileName = getImportFolder() + File.separator + record.getId() + ".xml";
	            ff.write(fileName);
	            
	            // create importobject for massimport
	            
	            String processTitle = record.getId() + "_" + (String) map.get("description");
	            // remove non-ascii characters for the sake of TIFF header limits
                String regex = ConfigurationHelper.getInstance().getProcessTitleReplacementRegex();
                String filteredTitle = processTitle.replaceAll(regex, "_");
                io.setProcessTitle(filteredTitle);
	            io.setMetsFilename(fileName);
	            io.setImportReturnValue(ImportReturnValue.ExportFinished);
	            
	            // Barcode as property
	            Processproperty ppBarcode = new Processproperty();
	            ppBarcode.setTitel("barcode");
	            ppBarcode.setWert((String) map.get("barcode"));
	            io.getProcessProperties().add(ppBarcode);
	            
	            // Series as property
	            Processproperty ppSeries = new Processproperty();
	            ppSeries.setTitel("series_id");
	            ppSeries.setWert((String) map.get("series_id"));
	            io.getProcessProperties().add(ppSeries);
	            
	            // consignment_no as property
	            Processproperty ppCons = new Processproperty();
	            ppCons.setTitel("consignment_no");
	            ppCons.setWert((String) map.get("consignment_no"));
	            io.getProcessProperties().add(ppCons);
	            
	            // unit_no as property
	            Processproperty ppUnitNo = new Processproperty();
	            ppUnitNo.setTitel("unit_no");
	            ppUnitNo.setWert((String) map.get("unit_no"));
	            io.getProcessProperties().add(ppUnitNo);
	            
	            // unit_Item_code as property
	            Processproperty ppItemCode = new Processproperty();
	            ppItemCode.setTitel("unit_Item_code");
	            ppItemCode.setWert((String) map.get("unit_Item_code"));
	            io.getProcessProperties().add(ppItemCode);
	            
	            // description as property
	            Processproperty ppDesciption = new Processproperty();
	            ppDesciption.setTitel("description");
	            ppDesciption.setWert((String) map.get("description"));
	            io.getProcessProperties().add(ppDesciption);
	            
	        } catch (WriteException | PreferencesException | MetadataTypeNotAllowedException | TypeNotAllowedForParentException e) {
	        	io.setImportReturnValue(ImportReturnValue.WriteError);
	        }
	    	answer.add(io);
    	}
    	 // end of all csv rows
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
