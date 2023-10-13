package utils

import (
	"context"
	"os"

	"cloud.google.com/go/storage"
	logger "github.com/farovictor/GCSDownloader/src/logging"
	"google.golang.org/api/option"
)

// Default address in case none is set
const emulator string = "localhost:9023"
const EMULATOR_ENVVAR_NAME = "STORAGE_EMULATOR_HOST"

type AuthType string

const (
	// Uses a storage emulator. Pass the address to env var `STORAGE_EMULATOR_HOST`
	// Check for more info: https://github.com/oittaa/gcp-storage-emulator
	Emulator AuthType = "emulator"
	// Expects a credential file
	File = "file"
	// Expects you provide a json credential string
	JsonCred = "json"
)

// This serves development process. It should not be called in prod
func setEmulator(address string) error {
	var err error
	// If no address provided
	if address == "" {
		// Check if env var is set
		if val := os.Getenv(EMULATOR_ENVVAR_NAME); val == "" {
			logger.WarningLogger.Printf("No emulator address found at %s\n", EMULATOR_ENVVAR_NAME)
			// Set STORAGE_EMULATOR_HOST environment variable.
			err = os.Setenv(EMULATOR_ENVVAR_NAME, emulator)
			if err != nil {
				// TODO: Handle error.
				// logger.ErrorLogger.Fatalln(err)
				return err
			}
			logger.InfoLogger.Println("Emulator storage expected on", emulator)
		}
	} else {
		err = os.Setenv(EMULATOR_ENVVAR_NAME, address)

	}
	return err
}

type IncorrectAuth struct {
	message string
}

// Error for cases where auth type set does not match the ones specified as AuthType
func (a *IncorrectAuth) Error() string {
	return "Incorrect AuthType"
}

// Wrapper that will manage how to open a client based on AuthType set
func OpenClient(ctx context.Context, auth AuthType, content string) (*storage.Client, error) {
	var client *storage.Client
	var err error

	switch auth {
	case Emulator:
		if err := setEmulator(content); err != nil {
			break
		}
		client, err = storage.NewClient(ctx)
	case File:
		// TODO: Set proper credential file
		credFile := option.WithCredentialsFile("")
		client, err = storage.NewClient(ctx, credFile)
	case JsonCred:
		// TODO: Set proper json content
		credJson := option.WithCredentialsJSON([]byte("somecred"))
		client, err = storage.NewClient(ctx, credJson)
	}
	return client, err
}
