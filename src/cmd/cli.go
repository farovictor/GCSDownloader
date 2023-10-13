package cmd

import (
	"fmt"
	"os"

	logger "github.com/farovictor/GCSDownloader/src/logging"
	utils "github.com/farovictor/GCSDownloader/src/utils"
	"github.com/spf13/cobra"
)

var (
	// Build vars
	Version   string
	GitCommit string
	BuildTime string

	// Variables from cli
	authType                utils.AuthType = "file"
	authHolder              string
	blobName                string
	blobPrefix              string
	bucketName              string
	concurrency             int32
	concurrencyDefault      int32 = 32
	errorFileTracker        bool
	errorFileTrackerPath    string
	flattenDirectory        bool
	flattenDirectoryDefault bool = false
	logLevel                string
	logLevelDefault         string = "info"
	outputPath              string
	outputPathDefault       string = "."
)

// Root Command (does nothing, only prints nice things)
var rootCmd = &cobra.Command{
	Short:   "This project aims to support mongodb loading pipelines",
	Version: Version,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("For more info, visit: https://github.com/farovictor/GCSDownloader\n")
		fmt.Printf("Git Commit: %s\n", GitCommit)
		fmt.Printf("Built: %s\n", BuildTime)
		fmt.Printf("Version: %s\n", Version)
		fmt.Printf("Log-Level: %v\n", logLevel)
	},
}

// Download specified blob
var downloadBlobCmd = &cobra.Command{
	Use:     "dowload-blob",
	Version: rootCmd.Version,
	Short:   "Downlaod blob",
	Run:     downloadBlob,
}

// Download blobs in batch
var downloadBatchesCmd = &cobra.Command{
	Use:     "download-batch",
	Version: rootCmd.Version,
	Short:   "Downlaod Batches",
	Run:     downloadBatches,
}

// Executes cli
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.ErrorLogger.Printf("%v %s\n", os.Stderr, err)
		println()
		os.Exit(1)
	}
}

func init() {

	// Root command flags setup
	rootCmd.PersistentFlags().StringVarP(&bucketName, "bucket-name", "b", "", "Bucket name")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", logLevelDefault, "Set a max log level")
	rootCmd.PersistentFlags().StringVarP((*string)(&authType), "auth-type", "a", utils.File, "Set what authentication type to use")
	rootCmd.PersistentFlags().StringVarP(&authHolder, "auth", "c", "", "Pass authentication")

	// Download Blob
	downloadBlobCmd.PersistentFlags().StringVarP(&blobName, "blob-name", "b", "", "Blob name")
	downloadBlobCmd.PersistentFlags().StringVarP(&outputPath, "output-path", "o", outputPathDefault, "Output path to save the dowloaded blob")
	// NOTE: Flattening in download-blob is set too true by default
	downloadBlobCmd.PersistentFlags().BoolVar(&flattenDirectory, "flatten-directory", !flattenDirectoryDefault, "Flatten directory on dump")

	// Download Prefix
	downloadBatchesCmd.PersistentFlags().Int32VarP(&concurrency, "num-concurrency", "x", concurrencyDefault, "Number of concurrent workers")
	downloadBatchesCmd.PersistentFlags().StringVarP(&outputPath, "output-path", "o", outputPathDefault, "Output path to save dowloaded blobs")
	downloadBatchesCmd.PersistentFlags().StringVarP(&blobPrefix, "blob-prefix", "p", "", "Blob prefix to match")
	downloadBatchesCmd.PersistentFlags().BoolVar(&flattenDirectory, "flatten-directory", flattenDirectoryDefault, "Flatten directory on dump")

	// Attaching commands to root
	rootCmd.AddCommand(downloadBlobCmd)
	rootCmd.AddCommand(downloadBatchesCmd)
}
