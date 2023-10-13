package utils

import (
	"os"
	"path/filepath"
	"strings"

	logger "github.com/farovictor/GCSDownloader/src/logging"
)

// Assemble file path
func CleanPath(blobPath string) string {

	path := blobPath

	// Remove last character in case is a slash
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
		// Recursive search in case there is many slashes
		path = CleanPath(path)
	}

	return path
}

// Create file from blob name
func CreateFileFromBlob(blobPath string, outputPath string, bc *BatchHelper) (string, error) {

	var finalPath string

	p := strings.Split(blobPath, "/")

	if bc.FlattenPath {

		blobName := p[len(p)-1]

		o := CleanPath(outputPath)

		finalPath = filepath.Join(o, blobName)

	} else {

		prefix := strings.Join(p[:len(p)-1], "/")
		folderStructure := filepath.Join(outputPath, prefix)

		// Check cached paths
		if exists := bc.ExistsInCachedPaths(folderStructure); exists == false {

			// Check if folder exists
			if _, err := os.Stat(folderStructure); err != nil {
				if os.IsNotExist(err) {
					// In case it does not exists, create one
					if err := os.MkdirAll(folderStructure, 0755); err != nil {
						// If not able to create, warn the error and flatten the path
						logger.InfoLogger.Println("Flattening directory for:", blobPath)
						blobName := p[len(p)-1]
						o := CleanPath(outputPath)
						finalPath = filepath.Join(o, blobName)
					} else {
						bc.AddingNewlyCreatedPath(folderStructure)
					}
				}
			}
		}

		blobName := p[len(p)-1]
		finalPath = filepath.Join(folderStructure, blobName)
	}
	return finalPath, nil
}
