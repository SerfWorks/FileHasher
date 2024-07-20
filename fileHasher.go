package fileHasher

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"io"
	"io/fs"
	"math"
	"os"
	"strings"
	"time"
)

type ManifestType int

//goland:noinspection GoSnakeCaseUsage
const (
	MF_Directory ManifestType = iota
	MF_File
	MF_ChunkedFile
	MF_Chunk
	MF_ChunkProxy
)

var ChunkSize = 1024 * 1024 * 32

type Manifest struct {
	Id          string                     `json:"id" bson:"id" required:"true" unique:"1"`
	BuildId     string                     `json:"buildId" bson:"buildId" required:"true" validate:"Build"`
	Created     time.Time                  `json:"created" bson:"created" required:"true"`
	Checksum    string                     `json:"checksum" bson:"checksum" required:"true"`
	Files       []ManifestElementFile      `json:"files" bson:"files"`
	Directories []ManifestElementDirectory `json:"directories" bson:"directories"`
}

func (m *Manifest) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	for _, file := range m.Files {
		if file.Name == pathElements[0] {
			return file.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	for _, directory := range m.Directories {
		if directory.Name == pathElements[0] {
			return directory.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *Manifest) GetChunkProxyAtPath(path string) *ManifestElementChunkProxy {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	for _, file := range m.Files {
		if file.Name == pathElements[0] {
			return file.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	for _, directory := range m.Directories {
		if directory.Name == pathElements[0] {
			return directory.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *Manifest) GetFileAtPath(path string) *ManifestElementFile {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, file := range m.Files {
			if file.Name == pathElements[0] {
				return &file
			}
		}
		return nil
	}

	for _, dir := range m.Directories {
		if dir.Name == pathElements[0] {
			return dir.GetFileAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *Manifest) GetDirectoryAtPath(path string) *ManifestElementDirectory {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, dir := range m.Directories {
			if dir.Name == pathElements[0] {
				return &dir
			}
		}
		return nil
	}

	for _, dir := range m.Directories {
		if dir.Name == pathElements[0] {
			return dir.GetDirectoryAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

type ManifestData interface {
	GetType() ManifestType
	GetChecksum() string
}

type ManifestElement struct {
	Type     int    `json:"type" bson:"type"`
	Checksum string `json:"hash" bson:"hash"`
}

func (m *ManifestElement) GetType() ManifestType {
	return ManifestType(m.Type)
}

func (m *ManifestElement) GetChecksum() string {
	return m.Checksum
}

type ManifestFilePiece interface {
	IsProxy() bool
}

type ManifestElementChunk struct {
	ManifestElement
}

func buildChunk(checksum string, chunkTargetPath string) ManifestElementChunk {
	manifestChunk := ManifestElementChunk{}
	manifestChunk.Type = int(MF_Chunk)
	manifestChunk.Checksum = checksum
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
		Type:     int(temp["type"].(float64)),
		Checksum: temp["hash"].(string),
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

func (m *ManifestElementChunkProxy) UnmarshalBSON(data []byte) error {
	temp := make(map[string]interface{})
	err := bson.Unmarshal(data, &temp)
	if err != nil {
		fmt.Println("Failed to unmarshal BSON at line 208: ", err)
		return err
	}

	for _, chunk := range temp["chunks"].([]map[string]interface{}) {
		var chunkData []byte
		chunkData, err = bson.Marshal(chunk)
		if err != nil {
			fmt.Println("Failed to marshal chunk at line 216: ", err)
			return err
		}
		if ManifestType(int(chunk["type"].(float64))) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = bson.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		} else {
			tempChunk := ManifestElementChunkProxy{}
			err = bson.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		}
	}
	return nil
}

func (m *ManifestElementChunkProxy) IsProxy() bool {
	return true
}

func (m *ManifestElementChunkProxy) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}
	if len(pathElements) == 1 {
		for _, element := range m.Chunks {
			if chunk, ok := element.(*ManifestElementChunk); ok && chunk.Checksum == pathElements[0] {
				return chunk
			}
		}
		return nil
	}

	for _, element := range m.Chunks {
		if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
			return chunk.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementChunkProxy) GetChunkProxyAtPath(path string) *ManifestElementChunkProxy {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}
	if len(pathElements) == 1 {
		for _, element := range m.Chunks {
			if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
				return chunk
			}
		}
		return nil
	}
	for _, element := range m.Chunks {
		if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
			return chunk.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}
	return nil
}

func (m *ManifestElementChunkProxy) BuildProxy(chunkTargetPath, currentPath string, priorManifest *Manifest) chan error {
	output := make(chan error)
	go func() {
		priorProxy := m.GetChunkProxyAtPath(currentPath + "\\" + m.Checksum)
		if priorProxy != nil {
			if priorProxy.Checksum == m.Checksum {
				m.Chunks = priorProxy.Chunks
				output <- nil
				return
			}
		}

		m.Type = int(MF_ChunkProxy)
		file, err := os.Open(chunkTargetPath + "\\" + m.Checksum)
		if err != nil {
			fmt.Println("Failed to open proxy file at line 295: ", err)
			output <- err
			return
		}
		defer func() {
			_ = file.Close()
			err = os.Remove(chunkTargetPath + "\\" + m.Checksum)
			if err != nil {
				fmt.Println("Failed to remove proxy file at line 303: " + err.Error())
			}
		}()
		_, _ = file.Seek(0, 0)
		splitChan := splitFile(file, chunkTargetPath)
		splitData := <-(*splitChan)
		if splitData.err != nil {
			output <- splitData.err
			return
		}
		if splitData.HalfSize <= int64(ChunkSize) {
			leftChunkData := buildChunk(splitData.LeftFileChecksum, chunkTargetPath)
			m.Chunks = append(m.Chunks, &leftChunkData)

			rightChunkData := buildChunk(splitData.RightFileChecksum, chunkTargetPath)
			m.Chunks = append(m.Chunks, &rightChunkData)

			close(output)
			return
		}

		leftProxy := ManifestElementChunkProxy{}
		leftProxy.Checksum = splitData.LeftFileChecksum
		leftChan := leftProxy.BuildProxy(chunkTargetPath, currentPath+"\\"+m.Checksum, priorManifest)

		rightProxy := ManifestElementChunkProxy{}
		rightProxy.Checksum = splitData.RightFileChecksum
		rightChan := rightProxy.BuildProxy(chunkTargetPath, currentPath+"\\"+m.Checksum, priorManifest)

		err = <-leftChan
		if err != nil {
			fmt.Println("Failed to build left proxy at line 333: ", err)
			output <- err
			return
		}
		err = <-rightChan
		if err != nil {
			fmt.Println("Failed to build right proxy at line 339: ", err)
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
}

func (m *ManifestElementFile) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, element := range m.Chunks {
			if chunk, ok := element.(*ManifestElementChunk); ok && chunk.Checksum == pathElements[0] {
				return chunk
			}
		}
		return nil
	}

	for _, element := range m.Chunks {
		if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
			return chunk.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementFile) GetChunkProxyAtPath(path string) *ManifestElementChunkProxy {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}
	if len(pathElements) == 1 {
		for _, element := range m.Chunks {
			if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
				return chunk
			}
		}
		return nil
	}
	for _, element := range m.Chunks {
		if chunk, ok := element.(*ManifestElementChunkProxy); ok && chunk.Checksum == pathElements[0] {
			return chunk.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementFile) UnmarshalJSON(data []byte) error {
	temp := make(map[string]interface{})
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.Name = temp["name"].(string)
	m.Type = int(temp["type"].(float64))
	m.Checksum = temp["hash"].(string)
	for _, chunk := range temp["chunks"].([]map[string]interface{}) {
		var chunkData []byte
		chunkData, err = json.Marshal(chunk)
		if err != nil {
			fmt.Println("Failed to marshal chunk at line 421: ", err)
			return err
		}
		if ManifestType(int(chunk["type"].(float64))) == MF_Chunk {
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

func (m *ManifestElementFile) UnmarshalBSON(data []byte) error {
	temp := make(map[string]interface{})
	err := bson.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	for _, chunk := range temp["chunks"].([]map[string]interface{}) {
		var chunkData []byte
		chunkData, err = bson.Marshal(chunk)
		if err != nil {
			return err
		}
		if ManifestType(int(chunk["type"].(float64))) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = bson.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, &tempChunk)
		} else {
			tempChunk := ManifestElementChunkProxy{}
			err = bson.Unmarshal(chunkData, &tempChunk)
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

func (m *ManifestElementDirectory) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) < 2 {
		return nil
	}

	if len(pathElements) == 2 {
		for _, element := range m.Elements {
			if file, ok := element.(*ManifestElementFile); ok && file.Name == pathElements[0] {
				return file.GetChunkAtPath(strings.Join(pathElements[2:], "\\"))
			}
		}
		return nil
	}
	for _, element := range m.Elements {
		if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
			return dir.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
		if file, ok := element.(*ManifestElementFile); ok && file.Name == pathElements[0] {
			return file.GetChunkAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}
	return nil
}

func (m *ManifestElementDirectory) GetChunkProxyAtPath(path string) *ManifestElementChunkProxy {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) < 2 {
		return nil
	}

	if len(pathElements) == 2 {
		for _, element := range m.Elements {
			if file, ok := element.(*ManifestElementFile); ok && file.Name == pathElements[0] {
				return file.GetChunkProxyAtPath(strings.Join(pathElements[2:], "\\"))
			}
		}
	}

	for _, element := range m.Elements {
		if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
			return dir.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
		if file, ok := element.(*ManifestElementFile); ok && file.Name == pathElements[0] {
			return file.GetChunkProxyAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementDirectory) GetFileAtPath(path string) *ManifestElementFile {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, element := range m.Elements {
			if file, ok := element.(*ManifestElementFile); ok && file.Name == pathElements[0] {
				return file
			}
		}
		return nil
	}

	for _, element := range m.Elements {
		if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
			return dir.GetFileAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementDirectory) GetDirectoryAtPath(path string) *ManifestElementDirectory {
	pathElements := strings.Split(path, "\\")
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, element := range m.Elements {
			if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
				return dir
			}
		}
	}
	for _, element := range m.Elements {
		if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
			return dir.GetDirectoryAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementDirectory) UnmarshalJSON(data []byte) error {
	temp := make(map[string]interface{})
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.Name = temp["name"].(string)
	m.Type = int(temp["type"].(float64))
	m.Checksum = temp["hash"].(string)
	for _, directory := range temp["elements"].([]map[string]interface{}) {
		var directoryData []byte
		directoryData, err = json.Marshal(directory)
		if err != nil {
			return err
		}
		if ManifestType(int(temp["type"].(float64))) == MF_Directory {
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

func (m *ManifestElementDirectory) UnmarshalBSON(data []byte) error {
	temp := make(map[string]interface{})
	err := bson.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	m.Name = temp["name"].(string)
	m.Type = int(temp["type"].(float64))
	m.Checksum = temp["hash"].(string)
	for _, directory := range temp["elements"].([]map[string]interface{}) {
		var directoryData []byte
		directoryData, err = bson.Marshal(directory)
		if err != nil {
			return err
		}
		if directory["type"].(ManifestType) == MF_Directory {
			tempDirectory := ManifestElementDirectory{}
			err = bson.Unmarshal(directoryData, &tempDirectory)
			m.Elements = append(m.Elements, &tempDirectory)
		} else {
			tempFile := ManifestElementFile{}
			err = bson.Unmarshal(directoryData, &tempFile)
			m.Elements = append(m.Elements, &tempFile)
		}
	}
	return nil
}

type WorkingManifest struct {
	Path        string                     `json:"path" bson:"path"`
	Directories []ManifestElementDirectory `json:"directories" bson:"directories"`
	Files       []ManifestElementFile      `json:"files" bson:"files"`
	Checksum    string                     `json:"checksum" bson:"checksum"`
}

type fileSplitOutput struct {
	LeftFileChecksum  string
	RightFileChecksum string
	HalfSize          int64
	Size              int64
	err               error
}

func splitFile(file *os.File, chunkTargetPath string) *chan fileSplitOutput {
	output := make(chan fileSplitOutput)
	go func() {
		var leftFile, rightFile *os.File
		splitData := fileSplitOutput{}
		sourceStat, err := file.Stat()
		if err != nil {
			fmt.Println("Failed to stat file at line 645: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		if sourceStat.Mode().IsRegular() == false {
			fmt.Println("Not a regular file at line 651")
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
			fmt.Println("Failed to create left file at line 666: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		_, err = io.CopyN(leftFile, file, halfSize)
		if err != nil {
			fmt.Println("Failed to copy right file at line 673: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		_ = leftFile.Close()

		rightFile, err = os.Create(rightFileName)
		if err != nil {
			fmt.Println("Failed to create right file at line 682: ", err)
			splitData.err = err
			output <- splitData
			return
		}

		_, err = file.Seek(halfSize, 0)
		if err != nil {
			fmt.Println("Failed to seek right file at line 690: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		_, err = io.Copy(rightFile, file)
		if err != nil {
			fmt.Println("Failed to copy right file at line 697: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		_ = rightFile.Close()

		leftFile, err = os.Open(leftFileName)
		if err != nil {
			fmt.Println("Failed to open left file at line 706: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		hash := md5.New()
		if _, err = io.Copy(hash, leftFile); err != nil {
			fmt.Println("Failed to copy left file at line 713: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		splitData.LeftFileChecksum = hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Left Checksum: ", splitData.LeftFileChecksum)
		hash.Reset()

		rightFile, err = os.Open(rightFileName)
		if err != nil {
			fmt.Println("Failed to open right file at line 724: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		if _, err = io.Copy(hash, rightFile); err != nil {
			fmt.Println("Failed to copy right file at line 730: ", err)
			splitData.err = err
			output <- splitData
			return
		}

		splitData.RightFileChecksum = hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Right Checksum: ", splitData.RightFileChecksum)

		err = rightFile.Close()
		if err != nil {
			fmt.Println("Failed to close right file at line 741: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		err = leftFile.Close()
		if err != nil {
			fmt.Println("Failed to close left file at line 748: ", err)
			splitData.err = err
			output <- splitData
			return
		}
		err = os.Rename(leftFileName, chunkTargetPath+"\\"+splitData.LeftFileChecksum)
		if err != nil {
			fmt.Println("Failed to rename left file at line 755: ", err)
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
	element      ManifestElementFile
	checksumHash []byte
	err          error
}

func parseFile(filePath, chunkTargetPath, currentPath string, priorManifest *Manifest) *chan parseFileOutput {
	output := make(chan parseFileOutput)

	go func() {
		manifestOutput := parseFileOutput{}
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Failed to open file at line 783: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		var fileInfo fs.FileInfo
		fileInfo, err = file.Stat()
		if err != nil {
			fmt.Println("Failed to stat file at line 795: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		manifestOutput.element.Name = fileInfo.Name()
		manifestOutput.element.Type = int(MF_File)

		hash := md5.New()
		if _, err = io.Copy(hash, file); err != nil {
			fmt.Println("Failed to copy file at line 803: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		manifestOutput.element.Checksum = hex.EncodeToString(hash.Sum(nil))
		if priorManifest != nil {
			priorFile := priorManifest.GetFileAtPath(currentPath + "\\" + manifestOutput.element.Name)
			if priorFile != nil {
				if priorFile.Checksum == manifestOutput.element.Checksum {
					manifestOutput.element = *priorFile
					output <- manifestOutput
					return
				}
			}
		}

		defer file.Close()
		if fileInfo.Size() <= int64(ChunkSize) {
			continueParse := true
			if priorManifest != nil {
				priorFile := priorManifest.GetFileAtPath(currentPath + "\\" + manifestOutput.element.Name)
				if priorFile != nil {
					if priorFile.Checksum == manifestOutput.element.Checksum {
						continueParse = false
					}
				}
			}

			if continueParse {
				var newFile *os.File
				newFile, err = os.Create(chunkTargetPath + "\\" + manifestOutput.element.Checksum)
				if err != nil {
					fmt.Println("Failed to create file at line 837: ", err)
					manifestOutput.err = err
					output <- manifestOutput
					return
				}

				defer newFile.Close()

				_, err = io.Copy(newFile, file)
				if err != nil {
					fmt.Println("Failed to copy file at line 847: ", err)
					manifestOutput.err = err
					output <- manifestOutput
					return
				}
			}

			output <- manifestOutput
			return
		}

		_, _ = file.Seek(0, 0)
		baseChannel := splitFile(file, chunkTargetPath)
		baseData := <-(*baseChannel)
		if baseData.err != nil {
			fmt.Println("Failed to split file at line 862: ", baseData.err)
			manifestOutput.err = baseData.err
			output <- manifestOutput
			return
		}

		if baseData.HalfSize <= int64(ChunkSize) {
			leftChunkData := ManifestElementChunk{}
			leftChunkData.Type = int(MF_Chunk)
			leftChunkData.Checksum = baseData.LeftFileChecksum
			manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &leftChunkData)

			rightChunkData := ManifestElementChunk{}
			rightChunkData.Type = int(MF_Chunk)
			rightChunkData.Checksum = baseData.RightFileChecksum
			manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &rightChunkData)

			if priorManifest != nil {
				priorLeftChunkData := priorManifest.GetChunkAtPath(currentPath + "\\" + manifestOutput.element.Name + "\\" + baseData.LeftFileChecksum)
				if priorLeftChunkData != nil {
					if priorLeftChunkData.Checksum == baseData.LeftFileChecksum {
						_ = os.Remove(chunkTargetPath + "\\" + baseData.LeftFileChecksum)
					}
				}
				priorRightChunkData := priorManifest.GetChunkAtPath(currentPath + "\\" + manifestOutput.element.Name + "\\" + baseData.RightFileChecksum)
				if priorRightChunkData != nil {
					if priorRightChunkData.Checksum == baseData.RightFileChecksum {
						_ = os.Remove(chunkTargetPath + "\\" + baseData.RightFileChecksum)
					}
				}
			}
			output <- manifestOutput
			return
		}

		manifestOutput.element.Type = int(MF_ChunkedFile)

		leftProxy := ManifestElementChunkProxy{}
		leftProxy.Type = int(MF_Chunk)
		leftProxy.Checksum = baseData.LeftFileChecksum
		leftChan := leftProxy.BuildProxy(chunkTargetPath, currentPath+"\\"+manifestOutput.element.Name+"\\", priorManifest)
		rightProxy := ManifestElementChunkProxy{}
		rightProxy.Type = int(MF_Chunk)
		rightProxy.Checksum = baseData.RightFileChecksum
		rightChan := rightProxy.BuildProxy(chunkTargetPath, currentPath+"\\"+manifestOutput.element.Name+"\\", priorManifest)

		err = <-leftChan
		if err != nil {
			fmt.Println("Failed to build left proxy at line 912: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		err = <-rightChan
		if err != nil {
			fmt.Println("Failed to build right proxy at line 920: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &leftProxy)
		manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, &rightProxy)

		_ = os.Remove(chunkTargetPath + "\\" + baseData.LeftFileChecksum)
		_ = os.Remove(chunkTargetPath + "\\" + baseData.RightFileChecksum)

		manifestOutput.element.Checksum = hex.EncodeToString(hash.Sum(nil))
		output <- manifestOutput
	}()

	return &output
}

type parseDirectoryOutput struct {
	manifestElement ManifestElementDirectory
	checksumHash    []byte
	err             error
}

func parseDirectory(filePath, chunkTargetPath, currentPath string, priorManifest *Manifest) *chan parseDirectoryOutput {
	output := make(chan parseDirectoryOutput)

	go func() {
		manifestOutput := parseDirectoryOutput{}
		manifestOutput.manifestElement.Name = currentPath
		directory, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Failed to open directory at line 953: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		var stat fs.FileInfo
		stat, err = directory.Stat()
		if err != nil {
			fmt.Println("Failed to stat directory at line 961: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		if stat.IsDir() == false {
			manifestOutput.err = errors.New("not a directory")
			output <- manifestOutput
			return
		}
		manifestOutput.manifestElement.Name = stat.Name()
		items, _ := directory.ReadDir(-1)

		var fileChannels []*chan parseFileOutput
		var directoryChannels []*chan parseDirectoryOutput

		for _, item := range items {
			itemPath := filePath + "\\" + item.Name()
			if item.IsDir() {
				directoryChannels = append(
					directoryChannels,
					parseDirectory(itemPath, chunkTargetPath, currentPath+"\\"+stat.Name(), priorManifest),
				)
				continue
			}
			fileChannels = append(
				fileChannels,
				parseFile(itemPath, chunkTargetPath, currentPath+"\\"+stat.Name(), priorManifest),
			)
		}

		directoryHash := md5.New()

		for _, directoryChannel := range directoryChannels {
			directoryOutput := <-*directoryChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &directoryOutput.manifestElement)

			_, err = fmt.Fprintf(directoryHash, "%x %s\n", directoryOutput.checksumHash, directoryOutput.manifestElement.Name)
			if err != nil {
				fmt.Println("Failed to write directory hash at line 1000: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			if directoryOutput.err != nil {
				fmt.Println("Failed to parse directory at line 1007: ", directoryOutput.err)
				manifestOutput.err = directoryOutput.err
				output <- manifestOutput
				return
			}
		}

		for _, fileChannel := range fileChannels {
			fileOutput := <-*fileChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &fileOutput.element)

			_, err = fmt.Fprintf(directoryHash, "%x %s\n", fileOutput.checksumHash, fileOutput.element.Name)
			if err != nil {
				fmt.Println("Failed to write file hash at line 1020: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			if fileOutput.err != nil {
				fmt.Println("Failed to parse file at line 1027: ", fileOutput.err)
				manifestOutput.err = fileOutput.err
				output <- manifestOutput
				return
			}
		}

		manifestOutput.checksumHash = directoryHash.Sum(nil)
		manifestOutput.manifestElement.Checksum = hex.EncodeToString(manifestOutput.checksumHash)

		output <- manifestOutput
	}()

	return &output
}

func BuildNewManifest(filePath, chunkStoragePath string, priorManifest *Manifest) (*Manifest, error) {
	manifest := Manifest{}

	var directoryChannels []*chan parseDirectoryOutput
	var fileChannels []*chan parseFileOutput

	fileSys := os.DirFS(filePath)
	items, _ := fs.ReadDir(fileSys, ".")
	for _, item := range items {
		if item.IsDir() {
			directoryChannels = append(directoryChannels, parseDirectory(filePath+"\\"+item.Name(), chunkStoragePath, "", priorManifest))
		} else {
			fileChannels = append(fileChannels, parseFile(filePath+"\\"+item.Name(), chunkStoragePath, "", priorManifest))
		}
	}

	manifestHash := md5.New()

	for _, directoryChannel := range directoryChannels {
		if directoryChannel != nil {
			directoryOutput := <-*directoryChannel
			manifest.Directories = append(manifest.Directories, directoryOutput.manifestElement)
			if directoryOutput.err != nil {
				fmt.Println("Failed to parse directory at line 1066: ", directoryOutput.err)
				return &manifest, directoryOutput.err
			}
			_, err := fmt.Fprintf(manifestHash, "%x %s\n", directoryOutput.checksumHash, directoryOutput.manifestElement.Name)
			if err != nil {
				return &manifest, err
			}
		}
	}

	for _, fileChannel := range fileChannels {
		if fileChannel != nil {
			fileOutput := <-*fileChannel
			manifest.Files = append(manifest.Files, fileOutput.element)
			if fileOutput.err != nil {
				fmt.Println("Failed to parse file at line 1081: ", fileOutput.err)
				return &manifest, fileOutput.err
			}
			_, err := fmt.Fprintf(manifestHash, "%x %s\n", fileOutput.checksumHash, fileOutput.element.Name)
			if err != nil {
				return &manifest, err
			}
		}
	}

	manifest.Checksum = hex.EncodeToString(manifestHash.Sum(nil))

	return &manifest, nil
}
