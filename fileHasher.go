package fileHasher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
)

type ManifestType int

const (
	MF_Directory ManifestType = iota
	MF_File
	MF_ChunkedFile
	MF_Chunk
	MF_ChunkProxy
)

var ChunkSize = 1024 * 1024 * 32

type Checksum string

type ManifestData interface {
	GetType() ManifestType
	GetChecksum() Checksum
}

type ManifestElement struct {
	Type     ManifestType `json:"type" bson:"type"`
	Checksum Checksum     `json:"hash" bson:"hash"`
}

func (m *ManifestElement) GetType() ManifestType {
	return m.Type
}

func (m *ManifestElement) GetChecksum() Checksum {
	return m.Checksum
}

type ManifestFilePiece interface {
	IsProxy() bool
}

type ManifestElementChunk struct {
	ManifestElement
	Url string `json:"url" bson:"url"`
}

func buildChunk(checksum Checksum, chunkTargetPath string) ManifestElementChunk {
	manifestChunk := ManifestElementChunk{}
	manifestChunk.Type = MF_Chunk
	manifestChunk.Checksum = checksum
	manifestChunk.Url = fmt.Sprintf("%s\\%s", chunkTargetPath, string(checksum))
	return manifestChunk
}

func (m *ManifestElementChunk) IsProxy() bool {
	return false
}

type ManifestElementChunkProxy struct {
	ManifestElement
	Chunks []ManifestFilePiece `json:"chunks" bson:"chunks"`
}

func (m *ManifestElementChunkProxy) UnmarshalJSON(data []byte) error {
	temp := make(map[string]interface{})
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.ManifestElement = ManifestElement{
		Type:     temp["type"].(ManifestType),
		Checksum: temp["hash"].(Checksum),
	}
	for _, chunk := range temp["chunks"].([]map[string]interface{}) {
		var chunkData []byte
		chunkData, err = json.Marshal(chunk)
		if err != nil {
			return err
		}
		if chunk["type"].(ManifestType) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = json.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		} else {
			tempChunk := ManifestElementChunkProxy{}
			err = json.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		}
	}
	return nil
}

func (m *ManifestElementChunkProxy) IsProxy() bool {
	return true
}

func (m *ManifestElementChunkProxy) BuildProxy(chunkTargetPath string) chan error {
	output := make(chan error)
	go func() {
		m.Type = MF_ChunkProxy
		file, err := os.Open(chunkTargetPath + "\\" + string(m.Checksum))
		if err != nil {
			output <- err
			return
		}
		defer func() {
			_ = file.Close()
			err = os.Remove(chunkTargetPath + "\\" + string(m.Checksum))
			if err != nil {
				fmt.Println("Failed to remove proxy file: " + err.Error())
			}
		}()
		splitChan := splitFile(file, chunkTargetPath)
		splitData := <-(*splitChan)
		if splitData.err != nil {
			output <- splitData.err
			return
		}
		if splitData.HalfSize <= int64(ChunkSize) {
			leftChunkData := buildChunk(Checksum(splitData.LeftFileChecksum), chunkTargetPath)
			m.Chunks = append(m.Chunks, &leftChunkData)

			rightChunkData := buildChunk(Checksum(splitData.RightFileChecksum), chunkTargetPath)
			m.Chunks = append(m.Chunks, &rightChunkData)

			close(output)
			return
		}

		leftProxy := ManifestElementChunkProxy{}
		leftProxy.Checksum = Checksum(splitData.LeftFileChecksum)
		leftChan := leftProxy.BuildProxy(chunkTargetPath)

		rightProxy := ManifestElementChunkProxy{}
		rightProxy.Checksum = Checksum(splitData.RightFileChecksum)
		rightChan := rightProxy.BuildProxy(chunkTargetPath)

		err = <-leftChan
		if err != nil {
			output <- err
			return
		}
		err = <-rightChan
		if err != nil {
			output <- err
			return
		}

		m.Chunks = append(m.Chunks, &leftProxy)
		m.Chunks = append(m.Chunks, &rightProxy)

		close(output)

	}()

	return output
}

type ManifestElementFile struct {
	Name string `json:"name" bson:"name"`
	ManifestElement
	Chunks []ManifestFilePiece `json:"chunks,omitempty" bson:"chunks,omitempty"`
	Url    string              `json:"url,omitempty" bson:"url,omitempty"`
}

func (m *ManifestElementFile) UnmarshalJSON(data []byte) error {
	temp := make(map[string]interface{})
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.Name = temp["name"].(string)
	m.Type = temp["type"].(ManifestType)
	m.Checksum = temp["hash"].(Checksum)
	for _, chunk := range temp["chunks"].([]map[string]interface{}) {
		var chunkData []byte
		chunkData, err = json.Marshal(chunk)
		if err != nil {
			return err
		}
		if chunk["type"].(ManifestType) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = json.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		} else {
			tempChunk := ManifestElementChunkProxy{}
			err = json.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		}
	}
	return nil
}

type ManifestElementDirectory struct {
	Name string `json:"name" bson:"name"`
	ManifestElement
	Elements []ManifestData `json:"elements" bson:"elements"`
}

func (m *ManifestElementDirectory) UnmarshalJSON(data []byte) error {
	temp := make(map[string]interface{})
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.Name = temp["name"].(string)
	m.Type = temp["type"].(ManifestType)
	m.Checksum = temp["hash"].(Checksum)
	for _, directory := range temp["elements"].([]map[string]interface{}) {
		var directoryData []byte
		directoryData, err = json.Marshal(directory)
		if err != nil {
			return err
		}
		if directory["type"].(ManifestType) == MF_Directory {
			tempDirectory := ManifestElementDirectory{}
			err = json.Unmarshal(directoryData, &tempDirectory)
			m.Elements = append(m.Elements, &tempDirectory)
		} else {
			tempFile := ManifestElementFile{}
			err = json.Unmarshal(directoryData, &tempFile)
			m.Elements = append(m.Elements, &tempFile)
		}
	}
	return nil
}

type WorkingManifest struct {
	Path        string                     `json:"path" bson:"path"`
	Directories []ManifestElementDirectory `json:"directories" bson:"directories"`
	Files       []ManifestElementFile      `json:"files" bson:"files"`
	Checksum    Checksum                   `json:"checksum" bson:"checksum"`
}

type fileSplitOutput struct {
	LeftFileChecksum  string
	RightFileChecksum string
	HalfSize          int64
	err               error
}

func splitFile(file *os.File, chunkTargetPath string) *chan fileSplitOutput {
	output := make(chan fileSplitOutput)
	go func() {
		var leftFile, rightFile *os.File
		splitData := fileSplitOutput{}
		sourceStat, err := file.Stat()
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		if sourceStat.Mode().IsRegular() == false {
			splitData.err = errors.New("not a regular file")
			output <- splitData
			return
		}

		halfSize := int64(math.Floor(float64(sourceStat.Size()) / 2))
		splitData.HalfSize = halfSize

		basePath := chunkTargetPath + "\\" + sourceStat.Name()

		leftFileName, rightFileName := basePath+".left", basePath+".right"

		leftFile, err = os.Create(leftFileName)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		_, err = io.CopyN(leftFile, file, halfSize)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		leftFile.Close()

		rightFile, err = os.Create(rightFileName)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}

		_, err = file.Seek(halfSize, 0)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		_, err = io.Copy(rightFile, file)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		rightFile.Close()

		leftFile, err = os.Open(leftFileName)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		hash := sha256.New()
		if _, err = io.Copy(hash, leftFile); err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		splitData.LeftFileChecksum = hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Left Checksum: ", splitData.LeftFileChecksum)
		hash.Reset()

		rightFile, err = os.Open(rightFileName)
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		if _, err = io.Copy(hash, rightFile); err != nil {
			splitData.err = err
			output <- splitData
			return
		}

		splitData.RightFileChecksum = hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Right Checksum: ", splitData.RightFileChecksum)

		err = rightFile.Close()
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		err = leftFile.Close()
		if err != nil {
			splitData.err = err
			output <- splitData
			return
		}
		err = os.Rename(leftFileName, chunkTargetPath+"\\"+splitData.LeftFileChecksum)
		if err != nil {
			fmt.Println("Failed to rename left file: " + err.Error())
			fmt.Println(leftFileName)
			fmt.Println(chunkTargetPath + "\\" + splitData.RightFileChecksum)
			splitData.err = err
			output <- splitData
			return
		}
		_ = os.Rename(rightFileName, chunkTargetPath+"\\"+splitData.RightFileChecksum)

		output <- splitData
	}()

	return &output
}

type parseFileOutput struct {
	element ManifestElementFile
	err     error
}

func parseFile(filePath string, chunkTargetPath string) *chan parseFileOutput {
	output := make(chan parseFileOutput)

	go func() {
		manifestOutput := parseFileOutput{}
		file, err := os.Open(filePath)
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		manifestOutput.element.Name = file.Name()
		manifestOutput.element.Type = MF_File

		var fileInfo fs.FileInfo
		fileInfo, err = file.Stat()
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		defer file.Close()
		if fileInfo.Size() <= int64(ChunkSize) {
			hash := sha256.New()
			if _, err = io.Copy(hash, file); err != nil {
				manifestOutput.err = err
				output <- manifestOutput
				return
			}
			manifestOutput.element.Checksum = Checksum(hex.EncodeToString(hash.Sum(nil)))

			var newFile *os.File
			newFile, err = os.Create(chunkTargetPath + "\\" + string(manifestOutput.element.Checksum))
			if err != nil {
				manifestOutput.err = err
				output <- manifestOutput
				return
			}
			defer newFile.Close()

			_, err = io.Copy(newFile, file)
			if err != nil {
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			manifestOutput.element.Url = fmt.Sprintf("%s\\%s", chunkTargetPath, string(manifestOutput.element.Checksum))
			output <- manifestOutput
			return
		}

		baseChannel := splitFile(file, chunkTargetPath)
		baseData := <-(*baseChannel)
		if baseData.err != nil {
			manifestOutput.err = baseData.err
			output <- manifestOutput
			return
		}
		if baseData.HalfSize <= int64(ChunkSize) {

			leftChunkData := ManifestElementChunk{}
			leftChunkData.Type = MF_Chunk
			leftChunkData.Checksum = Checksum(baseData.LeftFileChecksum)
			leftChunkData.Url = fmt.Sprintf("%s/%s", chunkTargetPath, baseData.LeftFileChecksum)
			manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &leftChunkData)
			rightChunkData := ManifestElementChunk{}
			rightChunkData.Type = MF_Chunk
			rightChunkData.Checksum = Checksum(baseData.RightFileChecksum)
			rightChunkData.Url = fmt.Sprintf("%s/%s", chunkTargetPath, baseData.RightFileChecksum)
			manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &rightChunkData)

			output <- manifestOutput
			return
		}

		manifestOutput.element.Type = MF_ChunkedFile

		leftProxy := ManifestElementChunkProxy{}
		leftProxy.Type = MF_Chunk
		leftProxy.Checksum = Checksum(baseData.LeftFileChecksum)
		leftChan := leftProxy.BuildProxy(chunkTargetPath)
		rightProxy := ManifestElementChunkProxy{}
		rightProxy.Type = MF_Chunk
		rightProxy.Checksum = Checksum(baseData.RightFileChecksum)
		rightChan := rightProxy.BuildProxy(chunkTargetPath)
		err = <-leftChan
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		err = <-rightChan
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &leftProxy)
		manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &rightProxy)
		leftByteChecksum, err := hex.DecodeString(baseData.LeftFileChecksum)
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		rightByteChecksum, err := hex.DecodeString(baseData.RightFileChecksum)
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		hash := sha256.New()
		_, err = hash.Write(append(leftByteChecksum, rightByteChecksum...))
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		_ = os.Remove(chunkTargetPath + "\\" + baseData.LeftFileChecksum)
		_ = os.Remove(chunkTargetPath + "\\" + baseData.RightFileChecksum)

		manifestOutput.element.Checksum = Checksum(hex.EncodeToString(hash.Sum(nil)))
		output <- manifestOutput
	}()

	return &output
}

type parseDirectoryOutput struct {
	manifestElement ManifestElementDirectory
	err             error
}

func parseDirectory(filePath string, chunkTargetPath string) *chan parseDirectoryOutput {
	output := make(chan parseDirectoryOutput)

	go func() {
		manifestOutput := parseDirectoryOutput{}
		manifestDirectory := ManifestElementDirectory{}
		manifestDirectory.Type = MF_Directory
		directory, err := os.Open(filePath)
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		manifestOutput.manifestElement.Name = filePath
		var stat fs.FileInfo
		stat, err = directory.Stat()
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		if stat.IsDir() == false {
			manifestOutput.err = errors.New("not a directory")
			output <- manifestOutput
			return
		}
		items, _ := directory.ReadDir(-1)

		var fileChannels []*chan parseFileOutput
		var directoryChannels []*chan parseDirectoryOutput

		for _, item := range items {
			itemPath := filePath + "\\" + item.Name()
			if item.IsDir() {
				directoryChannels = append(directoryChannels, parseDirectory(itemPath, chunkTargetPath))
				continue
			}
			fileChannels = append(fileChannels, parseFile(itemPath, chunkTargetPath))
		}

		var totalChecksumBytes []byte

		for _, directoryChannel := range directoryChannels {
			directoryOutput := <-*directoryChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &directoryOutput.manifestElement)
			if directoryOutput.err != nil {
				manifestOutput.err = directoryOutput.err
				output <- manifestOutput
				return
			}

			var checksumBytes []byte
			checksumBytes, err = hex.DecodeString(string(directoryOutput.manifestElement.Checksum))
			if err != nil {
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			totalChecksumBytes = append(totalChecksumBytes, checksumBytes...)
		}

		for _, fileChannel := range fileChannels {
			fileOutput := <-*fileChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &fileOutput.element)
			if fileOutput.err != nil {
				manifestOutput.err = fileOutput.err
				output <- manifestOutput
				return
			}

			var checksumBytes []byte
			checksumBytes, err = hex.DecodeString(string(fileOutput.element.Checksum))
			if err != nil {
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			totalChecksumBytes = append(totalChecksumBytes, checksumBytes...)
		}

		hash := sha256.New()
		_, err = hash.Write(totalChecksumBytes)
		if err != nil {
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		manifestOutput.manifestElement.Checksum = Checksum(hex.EncodeToString(hash.Sum(nil)))

		output <- manifestOutput
	}()

	return &output
}

func BuildNewManifest(filePath string, chunkStoragePath string) (WorkingManifest, error) {
	manifest := WorkingManifest{}

	var directoryChannels []*chan parseDirectoryOutput
	var fileChannels []*chan parseFileOutput

	fileSys := os.DirFS(filePath)
	items, _ := fs.ReadDir(fileSys, ".")
	for _, item := range items {
		if item.IsDir() {
			directoryChannels = append(directoryChannels, parseDirectory(filePath+"\\"+item.Name(), chunkStoragePath))
		} else {
			fileChannels = append(fileChannels, parseFile(filePath+"\\"+item.Name(), chunkStoragePath))
		}
	}

	for _, directoryChannel := range directoryChannels {
		if directoryChannel != nil {
			directoryOutput := <-*directoryChannel
			manifest.Directories = append(manifest.Directories, directoryOutput.manifestElement)
			if directoryOutput.err != nil {
				return manifest, directoryOutput.err
			}
		} else {
			fmt.Println("Channel is nil")
		}
	}

	for _, fileChannel := range fileChannels {
		if fileChannel != nil {
			fileOutput := <-*fileChannel
			manifest.Files = append(manifest.Files, fileOutput.element)
			if fileOutput.err != nil {
				return manifest, fileOutput.err
			}
		}
	}

	return manifest, nil
}
