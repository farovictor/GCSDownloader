package cmd

import (
	"context"
	"io/ioutil"
	"sync"

	logger "github.com/farovictor/GCSDownloader/src/logging"
	utils "github.com/farovictor/GCSDownloader/src/utils"
	"github.com/spf13/cobra"
)

func downloadBlob(cmd *cobra.Command, args []string) {
	logger.Initialize(logLevel)

	if bucketName == "" {
		logger.ErrorLogger.Fatalln("Specify a bucket name")
	}

	if blobName == "" {
		logger.ErrorLogger.Fatalln("Specify a valid blob name")
	}

	ctx := context.Background()

	client, err := utils.OpenClient(ctx, authType, authHolder)
	if err != nil {
		logger.ErrorLogger.Fatalln(err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	reader, err := bucket.Object(blobName).NewReader(ctx)
	if err != nil {
		// TODO: Handle error
	}
	defer reader.Close()

	content, err := ioutil.ReadAll(reader)

	filePath, err := utils.CreateFileFromBlob(blobName, outputPath, nil)
	if err != nil {
		logger.ErrorLogger.Println(err)
	}

	err = ioutil.WriteFile(filePath, content, 0644)
	if err != nil {
		// TODO: Handle errors
	}
}

func downloadBatches(cmd *cobra.Command, args []string) {
	logger.Initialize(logLevel)

	if bucketName == "" {
		logger.ErrorLogger.Fatalln("Specify a bucket name")
	}

	ctx := context.Background()

	logger.InfoLogger.Println("Init batch collector")

	// Initializing a Error Collector
	bc := utils.BatchHelper{
		OutputPath: outputPath,
	}

	// Creating workers
	var wg sync.WaitGroup
	var pipe chan string = make(chan string)

	// Create client as usual.
	client, err := utils.OpenClient(ctx, authType, authHolder)
	if err != nil {
		logger.ErrorLogger.Fatalln(err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	// Distributin workers
	for i := 0; i < int(concurrency); i++ {
		wg.Add(1)
		go utils.DownloadBlobFromPipe(&ctx, &wg, pipe, bucket, &bc)
	}

	// Read files and send to channel
	total, err := utils.EmitBlobsWithPrefix(ctx, bucket, blobPrefix, pipe)
	if err != nil {
		logger.ErrorLogger.Fatalln(err)
	}
	// Closing pipe
	close(pipe)

	logger.InfoLogger.Printf("Sent %d files to %d workers\n", total, concurrency)

	// Wait until channel is drainned and all workers are done
	wg.Wait()

	// Printing percentual of errors at the end
	amountErrors := len(bc.FilesNotProcessed)
	perc := float32(amountErrors) / float32(total) * 100
	logger.InfoLogger.Printf("Done loading %d files (%.2f %% losses) - total %d\n", total-amountErrors, perc, total)

	if errorFileTracker {
		if err := bc.DumpNotProcessedList(errorFileTrackerPath); err != nil {
			logger.ErrorLogger.Fatalln(err)
		}
	}
}
