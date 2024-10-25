package com.cts.ioutils;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.io.FilenameUtils.removeExtension;

public class DeviationReportWriter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public void printAvvikRapport(String pathToCurrentFailedFolder, String fullFileName,String avvikRapport, boolean publishDeviationReport) {
        String fileName = FilenameUtils.removeExtension(fullFileName);
        String updatedFileName = fileName.replace("TimestampRead","Failed"); //will enable reprocessing of the file - (files with "TimestampRead" in the file name are ignored.
        logger.info("Publish Deviation Report = " + publishDeviationReport);

        boolean writeReport = false;
        if (publishDeviationReport) {
            writeReport = true;
        }
        if (writeReport) {
            BufferedWriter bw = null;
            FileWriter fw = null;
            String pathToOutBoundFile = null;
            try {
                pathToOutBoundFile = pathToCurrentFailedFolder;
                File failedDir = new File(pathToOutBoundFile);
                if(!failedDir.exists()){
                    failedDir.mkdir();
                }
                String fileExtension = getExtension(fileName);
                if (fileExtension.length() == 0 || fileExtension == null) {
                    pathToOutBoundFile = pathToOutBoundFile + updatedFileName + ".txt";
                } else {
                    pathToOutBoundFile = removeExtension(pathToOutBoundFile);
                    pathToOutBoundFile = pathToOutBoundFile + fileName + ".txt";
                }
                fw = new java.io.FileWriter(pathToOutBoundFile);
                bw = new BufferedWriter(fw);
                bw.write(avvikRapport);
            } catch (IllegalArgumentException ex) {
                logger.error("Error occurred while writing AvvikRapport. Message: {}", ex.getMessage());
            } catch (IOException e) {
                logger.error("Error occurred while writing AvvikRapport. Message: {}", e.getMessage());
            } finally {
                try {
                    if (bw != null)
                        bw.close();
                    if (fw != null)
                        fw.close();
                } catch (IOException ex) {
                    logger.error("Error occurred while closing writer stream. Message: {}", ex.getMessage());
                }
            }
            logger.info("AvviksRapport skrevet til: " + pathToOutBoundFile);
        }
        else{
            logger.info("AvviksRapport ikke bestilt/skrevet.");
        }
    }
}
