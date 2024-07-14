package fileHasher

import (
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

func BuildManifest(filePath string) (ManifestElementDirectory, error) {
	// Open the file
	fsys := os.DirFS(filePath)
	baseManifest := 
}
