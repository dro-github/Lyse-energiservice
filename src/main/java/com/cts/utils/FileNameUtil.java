package com.cts.utils;

import com.cts.logger.BasicConfApp;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class FileNameUtil {
    final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);
    private  String outputFileExtension;

    public FileNameUtil(String outputFileExtension) throws IOException {
        this.outputFileExtension = outputFileExtension;
    }

    public FileNameUtil(){
    }

    public String getUpdatedFileNameForStandPerHour(String currentFileName, String updatedName, int counter) throws IOException {
        String base = FilenameUtils.removeExtension(currentFileName);
        String extension = FilenameUtils.getExtension(currentFileName);
        String target = "mergedFile";
        String replacement = updatedName + counter;
        String processedName = base.replace(target,replacement);
        String updatedFileName = processedName + "." + extension;
        return updatedFileName;
    }

    public String getUpdatedFileName(String currentFileName) throws IOException {
        String base = FilenameUtils.removeExtension(currentFileName);
        String updatedFileName = base +  "." + outputFileExtension;
        return  updatedFileName;
    }

    public String markFileAsRead(Path p){
        String url = p.toString();
        String pathOnly = FilenameUtils.getFullPath(url);
        String nameOnly = FilenameUtils.getBaseName(url);
        if(nameOnly.contains("_Failed_")){
            nameOnly = removeTimestampRead(nameOnly);
        }
        String extension = FilenameUtils.getExtension(url);
        String newPath = pathOnly + nameOnly + "_TimestampRead_" + System.currentTimeMillis() + "." + extension;
        try {
            File f = new File(p.toString());
            FileUtils.moveFile(
                    FileUtils.getFile(f),
                    FileUtils.getFile(newPath));
        } catch (IOException e) {
            logger.error("Error occurred while renaming already read files. Message: {}",e.getMessage());
        }
        return newPath;
    }

    private String removeTimestampRead(String nameOnly) {
        String fullName = nameOnly;
        char c = '_';
        int posOfLast_ = fullName.lastIndexOf(c);
        String timestampToRemove = fullName.substring(posOfLast_ - 7); // remove: _Failed_xxxxxxxxxxxxx
        nameOnly = nameOnly.replace(timestampToRemove,"");
        return nameOnly;
    }
}
