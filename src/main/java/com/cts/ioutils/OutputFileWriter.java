package com.cts.ioutils;
import com.cts.utils.FileNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class OutputFileWriter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public void writeJsonFile(String pathToCurrentProcessedFilesFolder,List<String> strings, List<String> fileNames,String outputFileExtension) throws IOException {
        logger.info("Starting writeOutput.");
        BufferedWriter bw = null;
        FileWriter fw = null;
        File processedDir = new File(pathToCurrentProcessedFilesFolder);
        if(!processedDir.exists()){
            processedDir.mkdir();
        }
        int counterForFileNames  = 0;
        for (String oneFile : strings) {
            String currentFileName = fileNames.get(counterForFileNames);
            String updatedFileName = new FileNameUtil(outputFileExtension).getUpdatedFileName(currentFileName);
            counterForFileNames ++;
            try {
                fw = new java.io.FileWriter(pathToCurrentProcessedFilesFolder + updatedFileName);
                bw = new BufferedWriter(fw);
                bw.write(oneFile);
            } catch (IOException e) {
                logger.error("Error occurred while writing file. Message: {}", e.getMessage());
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
        }
        logger.info("Completed writeOutput.");
    }
}