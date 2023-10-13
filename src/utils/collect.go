package utils

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	logger "github.com/farovictor/GCSDownloader/src/logging"
	"google.golang.org/api/iterator"
)

// This Struct will collect all failing requests (file path reference)
type BatchHelper struct {
	mu                  sync.Mutex
	FilesNotProcessed   []string
	OutputPath          string
	FlattenPath         bool
	AlreadyCreatedPaths []string
}

// Mutex to track files (prevent data race)
func (b *BatchHelper) AddError(file string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.FilesNotProcessed = append(b.FilesNotProcessed, file)
}

// Dump the list
func (b *BatchHelper) DumpNotProcessedList(path string) error {
	if len(b.FilesNotProcessed) > 0 {
		filePath := fmt.Sprintf("%s/errors.log", path)

		content := strings.Join(b.FilesNotProcessed, "\n")

		err := ioutil.WriteFile(filePath, []byte(content), 0644)

		if err == nil {
			logger.InfoLogger.Println("List of not processed files is available in:", filePath)
		}
		return err
	}

	logger.InfoLogger.Println("No files presented error")

	return nil

}

// Adding existent created paths
func (b *BatchHelper) AddingNewlyCreatedPath(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.AlreadyCreatedPaths = append(b.AlreadyCreatedPaths, path)
}

// Check if path exists in cached list of paths
func (b *BatchHelper) ExistsInCachedPaths(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	var found bool = false

	for _, p := range b.AlreadyCreatedPaths {
		if path == p {
			found = true
		}
	}

	return found
}

func BlobNameAssemble(pathPrefix string, namePrefix string, filePath string) string {
	var blobName string

	if namePrefix != "" {
		blobName += fmt.Sprintf("%s-", namePrefix)
	}

	s := strings.Split(filePath, string(os.PathSeparator))
	fileName := s[len(s)-1]

	blobName += fileName

	return fmt.Sprintf("%s/%s", pathPrefix, blobName)
}

func EmitBlobsWithPrefix(ctx context.Context, bucket *storage.BucketHandle, prefix string, pipe chan<- string) (int, error) {
	var counter int

	query := storage.Query{
		Prefix: prefix,
	}

	it := bucket.Objects(ctx, &query)

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
			return counter, err
		}
		pipe <- objAttrs.Name
		counter++
	}
	return counter, nil
}

// Download blob from channel
func DownloadBlobFromPipe(ctx *context.Context, wg *sync.WaitGroup, pipe <-chan string, bucket *storage.BucketHandle, bc *BatchHelper) {
	defer wg.Done()
	for blobPath := range pipe {
		reader, err := bucket.Object(blobPath).NewReader(*ctx)
		if err != nil {
			// TODO: Handle errors
			bc.AddError(blobPath)
			continue
		}

		content, err := ioutil.ReadAll(reader)
		if err != nil {
			// TODO: Handle errors
			bc.AddError(blobPath)
			continue
		}

		err = reader.Close()
		if err != nil {
			// TODO: Handle errors
			bc.AddError(blobPath)
			continue
		}

		filePath, err := CreateFileFromBlob(blobPath, bc.OutputPath, bc)
		if err != nil {
			logger.ErrorLogger.Println(err)
		}

		err = ioutil.WriteFile(filePath, content, 0644)
		if err != nil {

			// TODO: Handle errors
			logger.ErrorLogger.Println(err)
			logger.ErrorLogger.Printf("%s\n", filePath)

			bc.AddError(blobPath)
			continue
		}
	}
}
