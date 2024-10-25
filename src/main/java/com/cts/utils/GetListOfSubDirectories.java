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
import java.util.stream.Collectors;

public class GetListOfSubDirectories {

    private final static Logger logger = LoggerFactory.getLogger(BasicConfApp.class);

    public static List<Path> getListOfSubDirPerFormat(String pathToDir) throws IOException {
        logger.info("Scanning {} for existing sub-directories.", pathToDir);
        List<Path> subDirectoriesInBase = new ArrayList<>();
        Path baseDirectory = Paths.get(pathToDir);
        int depth = Paths.get(String.valueOf(baseDirectory)).getNameCount();
        try {
            subDirectoriesInBase =
                    Files.walk(baseDirectory)
                            .filter(it -> it.getNameCount() == (depth + 1))
                            .filter(Files::isDirectory)
                            .collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("Error while trying to traverse base directory to extract the list of sub-directories.");
            logger.error(e.getStackTrace().toString());
        }
        return subDirectoriesInBase;
    }
}
