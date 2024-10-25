package com.cts.utils;
import com.cts.logger.BasicConfApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class GetAllFilesFromTheDirectory {

    final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);
    private String outputFileExtension;

    public GetAllFilesFromTheDirectory(String outputFileExtension){
        this.outputFileExtension = outputFileExtension;
    }

    public List<Path> getAllFilePathsInFolder(String pathToFolder) throws IOException {
        int depth = Paths.get(pathToFolder).getNameCount();
        List<Path> paths = new ArrayList<>();
        FileNameUtil fileNameUtil = new FileNameUtil(outputFileExtension);
        try (Stream<Path> filePathStream = Files.walk(Paths.get(pathToFolder))) {
            filePathStream.filter(it -> it.getNameCount() == (depth + 1))
                    .filter(it -> !Files.isDirectory(it))
                    .filter(Files::isRegularFile)
                    .filter(it -> !it.toString().contains("TimestampRead"))
                    .forEach(filePath -> {
                        String newPath = fileNameUtil.markFileAsRead(filePath);
                        paths.add(Paths.get(newPath));
                    });
        } catch (IOException e) {
            logger.error("Error occurred while getting files from the input directory. Message: {}",e.getMessage());
        }
        return paths;
    }

    public List<Path> getAllFilePathsToMove(String pathToFolder) {
        List<Path> usedPathsToMove = new ArrayList<>();
        try (Stream<Path> filePathStream = Files.walk(Paths.get(pathToFolder))) {
            filePathStream.filter(it -> !Files.isDirectory(it))
                    .filter(Files::isRegularFile)
                    .filter(it -> it.toString().contains("TimestampRead"))
                    .forEach(filePath -> {
                        usedPathsToMove.add(filePath);
                    });
        } catch (IOException e) {
            logger.error("Error occurred while getting renamed files to remove from the input directory. Message: {}",e.getMessage());
        }
        return usedPathsToMove;
    }
}
