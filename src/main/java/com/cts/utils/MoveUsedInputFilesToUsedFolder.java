package com.cts.utils;
import com.cts.logger.BasicConfApp;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class MoveUsedInputFilesToUsedFolder {

    private final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);

    public static void moveFileFromOneDirectoryToAnother(String pathToCurrentUsedFilesFolder,List<Path> inputFiles){
        for (Path path : inputFiles){
            try {
                FileUtils.moveFileToDirectory(
                        FileUtils.getFile(path.toString()),
                        FileUtils.getFile(pathToCurrentUsedFilesFolder), true);
                logger.info("{} moved to {} .", path.getFileName(), pathToCurrentUsedFilesFolder);
            } catch (IOException e) {
                logger.error("Error occurred while moving the file to UsedFiles folder. Message: {}",e.getMessage());
            }
        }
    }
}

