package fileHasher

import (
	"fmt"
	"os"
)

type HashMap struct {
	Hash     string
	Children []HashMap
	URL      string
	FileName string
}

type ManifestElement struct {
	Type     string `json:"type" bson:"type"`
	Checksum string `json:"hash" bson:"hash"`
}

type ManifestElementDirectory struct {
	Name string `json:"name" bson:"name"`
	ManifestElement
	Children []ManifestElement
}

type ManifestElementChunk struct {
	ManifestElement
	Url string `json:"url" bson:"url"`
}

type ManifestElementFile struct {
	ManifestElement
	Chunks []ManifestElementChunk `json:"chunks" bson:"chunks"`
}

func BuildManifest(filePath string) ([]ManifestElement, error) {
	// Open the file
	fileSys := os.DirFS(filePath)
	_, err := fileSys.Open(filePath)
	if err != nil {
		return nil, err
	}
	fmt.Println("File opened successfully")
	return nil, nil
}
