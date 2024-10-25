package com.cts.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.List;

public class MergerFiles {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private File mergedFile;

    public MergerFiles(List<Path> inputFilesPerBorettslag){
        File[] inputFilesToBeMerged = initiateFileArray(inputFilesPerBorettslag);
        String pathToBorettslagFolder = String.valueOf(inputFilesPerBorettslag.get(0).getParent());
        String fullPathToMergedFileFolder = pathToBorettslagFolder + "\\" + "Merged_Temp";
        File mergedFolder = new File(fullPathToMergedFileFolder);
        if (!mergedFolder.exists()){
            mergedFolder.mkdirs();
        }
        mergedFile = new File(fullPathToMergedFileFolder + "\\" + "mergedFile.txt");
        mergedFile = mergeFiles(inputFilesToBeMerged, mergedFile);
    }

    private File[] initiateFileArray(List<Path> inputFilesPerBorettslag) {
        File[] files = new File[inputFilesPerBorettslag.size()];
        for (int i = 0; i < inputFilesPerBorettslag.size(); i++){
            files[i] = new File(String.valueOf(inputFilesPerBorettslag.get(i)));
        }
        return files;
    }

    private File mergeFiles(File[] inputFilesToBeMerged, File mergedFile) {
        FileWriter fStream;
        BufferedWriter out = null;
        try {
            fStream = new FileWriter(mergedFile, true);
            out = new BufferedWriter(fStream);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        for (File f : inputFilesToBeMerged) {
            logger.info("merging: {} into {}.", String.valueOf(f.toPath()),String.valueOf(mergedFile.toPath()));
            FileInputStream fis;
            try {
                fis = new FileInputStream(f);
                BufferedReader in = new BufferedReader(new InputStreamReader(fis));

                String aLine;
                while ((aLine = in.readLine()) != null) {
                    out.write(aLine);
                    out.newLine();
                }

                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mergedFile;
    }

    public File getMergedFile() {
        return mergedFile;
    }
}
