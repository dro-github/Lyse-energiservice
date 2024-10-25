package com.cts.controller;

import com.cts.ioutils.DeviationReportWriter;
import com.cts.ioutils.OutputFileWriter;
import com.cts.generator.ProcessInput;
import com.cts.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class Controller {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private StringBuilder avvikRapport;

    private long startProcessingTime;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${pathToBaseFolder}")
    private String pathToBaseFolder;

    @Value("${pathToFailedFolder}")
    private String pathToFailedFolder;

    @Value("${pathToTechyonIn_measurements}")
    private String pathToTechyonIn_measurements;

    @Value("${pathToUsedFilesFolder}")
    private String pathToUsedFilesFolder;

    @Value("${mBusFormat}")
    private String mBusNameReference;

    @Value("${mBusDateFormat}")
    private String mBusDateFormat;

    @Value("${forbruksImportFormat}")
    private String forbruksNameReference;

    @Value("${forbruksImportDateFormat}")
    private String forbruksDateFormat;

    @Value("${scanningIntervalInMinutes}")
    private String scanningIntervalInMinutes;

    @Value("${publishHourlyStand}")
    private boolean publishHourlyStand;

    @Value("${publishDeviationReport}")
    private boolean publishDeviationReport;

    @Value("${url_IsCustomer}")
    private String url_IsCustomer;

    @Value("${user}")
    private String userName;

    @Value("${password}")
    private String password;

    @Value("${outputFileExtension}")
    private String outputFileExtension;

    @Value("${max_transactions_per_file}")
    private int max_transactions_per_file;

    private boolean running = false;

    @Scheduled(fixedRateString = "${scanningInterval}")
    public void execute() {
        if (running) {
            return;
        }
        try {
            running = true;
            Thread.sleep(500);
            startProgram();
        } catch (IOException e) {
            logger.error("Error occurred. Message: {}", e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            running = false;
        }
    }

    private void startProgram() throws IOException {
        startProcessingTime = System.currentTimeMillis();
        List<Path> subDirPerFormat =  GetListOfSubDirectories.getListOfSubDirPerFormat(pathToBaseFolder); //pr Format
        Map<String, String> formatNameToFormatReference = new InitiateMappingForDateAndFormat("formatName",mBusNameReference,forbruksNameReference,mBusDateFormat,forbruksDateFormat).getFormatNameToFormatReference();
        Map<String,String> formatNameToDateFormat = new InitiateMappingForDateAndFormat("dateFormat",mBusNameReference,forbruksNameReference,mBusDateFormat,forbruksDateFormat). getFormatNameToFormatReference();
        if (subDirPerFormat != null && subDirPerFormat.size() > 0 && formatNameToFormatReference != null && formatNameToFormatReference.size() > 0 && formatNameToDateFormat != null && formatNameToDateFormat.size() > 0) {
            logger.info("Found {} sub-directories - one directory per file format.", subDirPerFormat.size());
            for (Path p : subDirPerFormat) {
                for (String formatName : formatNameToFormatReference.keySet()) {
                    if (p.toString().contains(formatNameToFormatReference.get(formatName))) {
                        logger.info("Scanning for sub directories for {} in {}.", formatName, p);
                        String partialPathToInputFolder = "\\" + formatNameToFormatReference.get(formatName) + "\\";
                        String fullPathToCurrentFormatFolder = pathToBaseFolder + partialPathToInputFolder;
                            GetAllFilesFromTheDirectory getAllFilesFromTheDirectory = new GetAllFilesFromTheDirectory(outputFileExtension);
                            List<Path> inputFilesPerFileType = getAllFilesFromTheDirectory.getAllFilePathsInFolder(fullPathToCurrentFormatFolder);
                            List<Path> mergedInputToProcess = new ArrayList<>();
                            logger.info("Found {} new input file/s.", inputFilesPerFileType.size());
                            logger.info("Will merge all {} input files into one file", inputFilesPerFileType.size());
                            //Merge all files into one
                            if(inputFilesPerFileType.size() > 0) {
                                File mergedFile = new MergerFiles(inputFilesPerFileType).getMergedFile();
                                mergedInputToProcess.add(mergedFile.toPath());
                            }
                            //Move used files, NB! do not move the merged file
                            moveUsedFiles(inputFilesPerFileType);

                            if (mergedInputToProcess.size() > 0) {
                                printOutput(mergedInputToProcess, formatNameToFormatReference.get(formatName),formatNameToFormatReference,formatNameToDateFormat);
                            }
                    }
                }
                logger.info("Completed processing {} in {} seconds.", p.toString(),(System.currentTimeMillis() - startProcessingTime) / 1000);
            }
            logger.info("Completed processing in {} seconds.", (System.currentTimeMillis() - startProcessingTime) / 1000);
            logger.info("Scan for new input files will be performed in {} minutes according to scheduling plan.", scanningIntervalInMinutes);
        }
    }

    private void printOutput(List<Path> inputFilesPerBorettslag, String currentFileFormat, Map<String, String> formatNameToFormatReference,Map<String,String> formatNameToDateFormat) throws IOException {
        FileNameUtil fnu = new FileNameUtil();
        for (Path pa : inputFilesPerBorettslag){
            List<String> filesNames = new ArrayList<>();
            String newPath = fnu.markFileAsRead(pa);
            File f = new File(newPath);
            String partialPathToFailedAndProcessedAndUsedFolder = String.valueOf(f.toPath().getParent().getParent());// the file to process is in the Merged sub-directory
            String lastPartOfPathToFailedFolder = pathToFailedFolder;
            String fullPathToFailedFolder = partialPathToFailedAndProcessedAndUsedFolder + lastPartOfPathToFailedFolder;
            String lastPartOfPathToUsedFolder = pathToUsedFilesFolder;
            String fullPathToUsedFolder = partialPathToFailedAndProcessedAndUsedFolder + lastPartOfPathToUsedFolder;
            avvikRapport = null;
            if(!hasMinimumLength(f)){
                logger.warn("Found input file: {} - Wrong or missing content. Will ignore this file.", f.getName());
                if (publishDeviationReport) {
                    writeAvviksRapport(f.getName(), fullPathToFailedFolder, publishDeviationReport);
                }
            }
            else {
                logger.info("Found input file: {}.", f.getName());
                ProcessInput processInput = new ProcessInput(f,currentFileFormat,url_IsCustomer,userName,password,jdbcTemplate,max_transactions_per_file,formatNameToFormatReference,formatNameToDateFormat,publishHourlyStand);
                List<String> jsonFilesFromCurrentInputFile = processInput.getLastHourlyStandReadyToPrintFiles();
                avvikRapport = processInput.getAvvikRapport();
                if (jsonFilesFromCurrentInputFile != null && jsonFilesFromCurrentInputFile.size() > 0) { //correct input rows were added
                    filesNames.add(f.getName());
                    if (jsonFilesFromCurrentInputFile.size() > 1){ // a hourlyStand file was created in addition to dailyStand file
                        int numOfAdditionalFiles = jsonFilesFromCurrentInputFile.size() - 1;// first file is the daily stand file
                        String addedFileNameForTS;
                        int counterForTimeseries = 0;
                        for (int i = 1; i <= numOfAdditionalFiles; i++) {
                            String fileHeader = jsonFilesFromCurrentInputFile.get(i).substring(0,80);
                            if (fileHeader.contains("timeSeriesMessages")) { // timeseries file.
                                counterForTimeseries ++;
                                addedFileNameForTS = fnu.getUpdatedFileNameForStandPerHour(f.getName(),"mergedFile-TS-", counterForTimeseries);
                                filesNames.add(addedFileNameForTS);
                            }
                            else if (fileHeader.contains("measurementMessages")) { // daily stand file - split-X (in case that the number of transaction exceeds the max_transactions_per_file).
                                addedFileNameForTS = fnu.getUpdatedFileNameForStandPerHour(f.getName(),"mergedFile-Split-", i - counterForTimeseries);
                                filesNames.add(addedFileNameForTS);
                            }
                        }
                    }
                    if (jsonFilesFromCurrentInputFile.size() > 0) {
                        new OutputFileWriter().writeJsonFile(pathToTechyonIn_measurements,jsonFilesFromCurrentInputFile,filesNames,outputFileExtension);
                        logger.info("Json file for input file {} written to {}.",f.getName(), pathToTechyonIn_measurements);
                    }
                    if (avvikRapport != null && avvikRapport.length() > 21) {
                        if (publishDeviationReport) {
                            writeAvviksRapport(f.getName(),fullPathToFailedFolder,publishDeviationReport);
                        }
                    }
                }
                else {
                    logger.info("No output created from file {}. This might be due to wrong input at all rows, corrupt file or no suitable input to be processed into json payload.",f.getName());
                    if (publishDeviationReport) {
                        writeAvviksRapport(f.getName(), fullPathToFailedFolder, publishDeviationReport);
                    }
                }
            }
            moveUsedMergedFile(fullPathToUsedFolder,f);
        }
        logger.info("Scan for new input files will be performed in {} minutes according to scheduling plan.",scanningIntervalInMinutes);
    }

    private boolean hasMinimumLength(File inputFile) {
        return inputFile.length() > 21;
    }

    private void writeAvviksRapport(String fileName,String pathToFailedFolder,boolean publishDeviationReport) {
        if(avvikRapport == null || avvikRapport.length() < 1) {
            new DeviationReportWriter().printAvvikRapport(pathToFailedFolder,fileName,"No output created from file '" + fileName + "'. This might be due to wrong input at all rows, corrupt file or no input to be processed into a transaction, (e.g. no stand hour between 23:00 and 01:00).",publishDeviationReport);
        }
        else {
            new DeviationReportWriter().printAvvikRapport(pathToFailedFolder,fileName, avvikRapport.toString(),publishDeviationReport);
        }
    }

    private void moveUsedFiles (List<Path> inputFilesPerFileType){
        for (Path pathToCurrentBorettslag : inputFilesPerFileType){
            String partialPathToFailedAndProcessedAndUsedFolder = String.valueOf(pathToCurrentBorettslag.getParent());
            String lastPartOfPathToUsedFolder = pathToUsedFilesFolder;
            String fullPathToUsedFolder = partialPathToFailedAndProcessedAndUsedFolder + lastPartOfPathToUsedFolder;
            logger.info("Moving used file {} to Used folder at {}.", String.valueOf(pathToCurrentBorettslag.toAbsolutePath()), fullPathToUsedFolder);
            logger.info("Starting to move used input files to {}.", fullPathToUsedFolder);
            List<Path> filesToMove = new GetAllFilesFromTheDirectory(outputFileExtension).getAllFilePathsToMove(String.valueOf(pathToCurrentBorettslag.toAbsolutePath()));
            MoveUsedInputFilesToUsedFolder.moveFileFromOneDirectoryToAnother(fullPathToUsedFolder, filesToMove);
            logger.info("Completed moving used file in {} seconds.", (System.currentTimeMillis() - startProcessingTime) / 1000);
        }
    }

    private void moveUsedMergedFile(String fullPathToUsedFolder, File usedMergedFile) {
        logger.info("Starting to move used merged input file to {}.", usedMergedFile.getPath());
        List<Path> mergedFileToMove = new ArrayList<>();
        String pathToUsedMergedFileToMove = usedMergedFile.getPath();
        mergedFileToMove.add(new File(pathToUsedMergedFileToMove).toPath());
        MoveUsedInputFilesToUsedFolder.moveFileFromOneDirectoryToAnother(fullPathToUsedFolder,mergedFileToMove);
        logger.info("Completed processing in {} seconds.",(System.currentTimeMillis() - startProcessingTime) / 1000);
    }
}
