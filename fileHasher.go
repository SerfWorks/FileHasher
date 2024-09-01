package fileHasher

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"io/fs"
	"math"
	"os"
	"slices"
	"strings"
	"time"
)

type ManifestType int

//goland:noinspection GoSnakeCaseUsage
const (
	MF_Directory ManifestType = iota
	MF_File
	MF_Chunk
	MF_ChunkProxy
)

var ChunkSize = 1024 * 1024 * 50

type Manifest struct {
	Id              string                     `json:"id" bson:"id" required:"true" unique:"1"`
	BuildId         string                     `json:"buildId" bson:"buildId" required:"true" validate:"Build"`
	Created         time.Time                  `json:"created" bson:"created" required:"true"`
	Checksum        string                     `json:"checksum" bson:"checksum" required:"true"`
	Files           []ManifestElementFile      `json:"files" bson:"files"`
	Directories     []ManifestElementDirectory `json:"directories" bson:"directories"`
	Size            int64                      `json:"size" bson:"size"`
	DetectedChanges bool                       `json:"-" bson:"-"`
}

func CleanEmptyElements(elements []string) []string {
	var outElements []string
	for _, element := range elements {
		if element != "" {
			outElements = append(outElements, element)
		}
	}
	return outElements
}

func (m *Manifest) GetListOfRequiredChunks(priorManifest *Manifest) []string {
	var requiredChunks []string
	var returnChannels []*chan []string

	for _, file := range m.Files {
		channel := make(chan []string)
		returnChannels = append(returnChannels, &channel)
		go func() {
			newChunks := file.GetListOfRequiredChunks("", priorManifest, true)
			channel <- newChunks
		}()
	}

	for _, directory := range m.Directories {
		channel := make(chan []string)
		returnChannels = append(returnChannels, &channel)
		go func() {
			newChunks := directory.GetListOfRequiredChunks("", priorManifest, true)
			channel <- newChunks
		}()
	}

	for _, channel := range returnChannels {
		newChunks := <-(*channel)
		requiredChunks = append(requiredChunks, newChunks...)
	}

	return requiredChunks
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

func (m *Manifest) GetFileAtPath(path string) *ManifestElementFile {
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
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
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
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

type ManifestFilePiece interface {
	IsProxy() bool
	GetListOfRequiredChunks(currentPath string, priorManifest *Manifest, forceExtract bool) []string
	GetChecksum() string
}

type ManifestElementChunk struct {
	Type     int    `json:"type" bson:"type"`
	Checksum string `json:"hash" bson:"hash"`
	Size     int64  `json:"size" bson:"size"`
}

func (m *ManifestElementChunk) GetListOfRequiredChunks(currentPath string, priorManifest *Manifest, forceExtract bool) []string {
	if forceExtract {
		return []string{m.Checksum}
	}
	priorChunk := priorManifest.GetChunkAtPath(currentPath + "\\" + m.Checksum)
	if priorChunk == nil {
		return []string{m.Checksum}
	}
	return []string{}
}

func (m *ManifestElementChunk) GetType() ManifestType {
	return ManifestType(m.Type)
}

func (m *ManifestElementChunk) GetChecksum() string {
	return m.Checksum
}

type ManifestElementFile struct {
	Name      string                 `json:"name" bson:"name"`
	Type      int                    `json:"type" bson:"type"`
	Checksum  string                 `json:"hash" bson:"hash"`
	Size      int64                  `json:"size" bson:"size"`
	Chunks    []ManifestElementChunk `json:"chunks,omitempty" bson:"chunks,omitempty"`
	ChunkSize int64                  `json:"chunkSize" bson:"chunkSize"`
}

func (m *ManifestElementFile) InstallSingleFile(installPath, chunkSourcePath string, duplicateChunks []string) error {
	file, err := os.Create(installPath + "\\" + m.Name)
	if err != nil {
		fmt.Println("Failed to create file installing single file: ", err)
		return err
	}
	defer file.Close()

	var sourceFile *os.File
	sourceFile, err = os.Open(chunkSourcePath + "\\" + m.Checksum)
	if err != nil {
		fmt.Println("Failed to open file installing single file: ", err)
		return err
	}
	defer sourceFile.Close()

	_, err = io.Copy(file, sourceFile)
	if err != nil {
		fmt.Println("Failed to copy file installing single file: ", err)
		return err
	}

	if !slices.Contains(duplicateChunks, m.Checksum) {
		os.Remove(chunkSourcePath + "\\" + m.Checksum)
	}

	return nil
}

func (m *ManifestElementFile) GetType() ManifestType {
	return ManifestType(m.Type)
}

func (m *ManifestElementFile) GetChecksum() string {
	return m.Checksum
}

func (m *ManifestElementFile) GetListOfRequiredChunks(currentPath string, priorManifest *Manifest, forceExtract bool) []string {
	var requiredChunks []string
	var returnChannels []*chan []string

	if forceExtract {
		if len(m.Chunks) == 0 {
			return []string{m.Checksum}
		}
		for _, chunk := range m.Chunks {
			channel := make(chan []string)
			returnChannels = append(returnChannels, &channel)
			go func() {
				newChunks := chunk.GetListOfRequiredChunks(currentPath, priorManifest, true)
				channel <- newChunks
			}()
		}
		for _, channel := range returnChannels {
			newChunks := <-(*channel)
			requiredChunks = append(requiredChunks, newChunks...)
		}
		return requiredChunks
	}

	priorFile := priorManifest.GetFileAtPath(currentPath + "\\" + m.Name)
	if priorFile == nil {
		if len(m.Chunks) == 0 {
			return []string{m.Checksum}
		}
		for _, chunk := range m.Chunks {
			channel := make(chan []string)
			returnChannels = append(returnChannels, &channel)
			go func() {
				newChunks := chunk.GetListOfRequiredChunks(currentPath, priorManifest, true)
				channel <- newChunks
			}()
		}
		for _, channel := range returnChannels {
			newChunks := <-(*channel)
			requiredChunks = append(requiredChunks, newChunks...)
		}
		return requiredChunks
	}

	if priorFile.Checksum == m.Checksum {
		return []string{}
	}

	if len(m.Chunks) == 0 {
		return []string{m.Checksum}
	}

	for _, chunk := range m.Chunks {
		channel := make(chan []string)
		returnChannels = append(returnChannels, &channel)
		go func() {
			newChunks := chunk.GetListOfRequiredChunks(currentPath, priorManifest, false)
			channel <- newChunks
		}()
	}

	for _, channel := range returnChannels {
		newChunks := <-(*channel)
		requiredChunks = append(requiredChunks, newChunks...)
	}

	return requiredChunks
}

func (m *ManifestElementFile) GetListOfAllChunks() []string {
	var chunkList []string

	if len(m.Chunks) == 0 {
		return []string{m.Checksum}
	}

	for _, chunk := range m.Chunks {
		chunkList = append(chunkList, chunk.GetChecksum())
	}

	return chunkList
}

func (m *ManifestElementFile) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, element := range m.Chunks {
			if element.Checksum == pathElements[0] {
				return &element
			}
		}
		return nil
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
	m.Size = int64(temp["size"].(float64))
	m.ChunkSize = int64(temp["chunkSize"].(float64))

	if temp["chunks"] == nil {
		return nil
	}
	for _, chunk := range temp["chunks"].([]interface{}) {
		castedChunk := chunk.(map[string]interface{})
		var chunkData []byte
		chunkData, err = json.Marshal(castedChunk)
		if err != nil {
			fmt.Println("Failed to marshal chunk at line 421: ", err)
			return err
		}
		if ManifestType(int(castedChunk["type"].(float64))) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = json.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, tempChunk)
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

	m.Name = temp["name"].(string)
	m.Type = int(temp["type"].(int32))
	m.Checksum = temp["hash"].(string)
	m.Size = temp["size"].(int64)
	m.ChunkSize = temp["chunkSize"].(int64)

	if temp["chunks"] == nil {
		return nil
	}

	for _, chunk := range temp["chunks"].(primitive.A) {
		castedChunk := chunk.(map[string]interface{})
		var chunkData []byte
		chunkData, err = bson.Marshal(castedChunk)
		if err != nil {
			return err
		}
		if ManifestType(int(castedChunk["type"].(int32))) == MF_Chunk {
			tempChunk := ManifestElementChunk{}
			err = bson.Unmarshal(chunkData, &tempChunk)
			m.Chunks = append(m.Chunks, tempChunk)
		}
	}
	return nil
}

type ManifestElementDirectory struct {
	Name     string         `json:"name" bson:"name"`
	Type     int            `json:"type" bson:"type"`
	Checksum string         `json:"hash" bson:"hash"`
	Elements []ManifestData `json:"elements,omitempty" bson:"elements,omitempty"`
	Size     int64          `json:"size" bson:"size"`
}

func (m *ManifestElementDirectory) GetNumFiles() int {
	var numFiles int
	for _, element := range m.Elements {
		switch element.GetType() {
		case MF_Directory:
			{
				numFiles += element.(*ManifestElementDirectory).GetNumFiles()
				break
			}
		case MF_File:
			{
				numFiles++
				break
			}
		default:
			{
				fmt.Println("Unknown element type getting num files")
				break
			}
		}
	}
	return numFiles
}

func (m *ManifestElementDirectory) GetListOfRequiredChunks(currentPath string, priorManifest *Manifest, forceExtract bool) []string {
	var requiredChunks []string
	if forceExtract {
		for _, element := range m.Elements {
			switch element.GetType() {
			case MF_Directory:
				{
					requiredChunks = append(requiredChunks, element.(*ManifestElementDirectory).GetListOfRequiredChunks(currentPath, priorManifest, true)...)
					break
				}
			case MF_File:
				{
					requiredChunks = append(requiredChunks, element.(*ManifestElementFile).GetListOfRequiredChunks(currentPath, priorManifest, true)...)
					break
				}
			default:
				{
					fmt.Println("Unknown element type getting list of required chunks from directory for forceExtract")
					break
				}
			}
		}
		return requiredChunks
	}

	priorDir := priorManifest.GetDirectoryAtPath(currentPath + "\\" + m.Name)
	if priorDir == nil {
		for _, element := range m.Elements {
			switch element.GetType() {
			case MF_Directory:
				{
					requiredChunks = append(requiredChunks, element.(*ManifestElementDirectory).GetListOfRequiredChunks(currentPath, priorManifest, true)...)
					break
				}
			case MF_File:
				{
					requiredChunks = append(requiredChunks, element.(*ManifestElementFile).GetListOfRequiredChunks(currentPath, priorManifest, true)...)
					break
				}
			default:
				{
					fmt.Println("Unknown element type getting list of required chunks from prior directory")
					break
				}
			}
		}
		return requiredChunks
	}

	if priorDir.Checksum == m.Checksum {
		return []string{}
	}

	for _, element := range m.Elements {
		switch element.GetType() {
		case MF_Directory:
			{
				requiredChunks = append(requiredChunks, element.(*ManifestElementDirectory).GetListOfRequiredChunks(currentPath, priorManifest, false)...)
				break
			}
		case MF_File:
			{
				requiredChunks = append(requiredChunks, element.(*ManifestElementFile).GetListOfRequiredChunks(currentPath, priorManifest, false)...)
				break
			}
		default:
			{
				fmt.Println("Unknown element type getting list of required chunks from directory")
				break
			}
		}
	}
	return requiredChunks
}

func (m *ManifestElementDirectory) GetListOfAllChunks() []string {
	var chunkList []string

	for _, element := range m.Elements {
		switch element.GetType() {
		case MF_Directory:
			{
				chunkList = append(chunkList, element.(*ManifestElementDirectory).GetListOfAllChunks()...)
				break
			}
		case MF_File:
			{
				chunkList = append(chunkList, element.(*ManifestElementFile).GetListOfAllChunks()...)
				break
			}
		default:
			{
				fmt.Println("Unknown element type getting list of required chunks from directory")
				break
			}
		}
	}
	return chunkList
}

func (m *ManifestElementDirectory) GetType() ManifestType {
	return ManifestType(m.Type)
}

func (m *ManifestElementDirectory) GetChecksum() string {
	return m.Checksum
}

func (m *ManifestElementDirectory) GetChunkAtPath(path string) *ManifestElementChunk {
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
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

func (m *ManifestElementDirectory) GetFileAtPath(path string) *ManifestElementFile {
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
	if len(pathElements) == 0 {
		return nil
	}

	if len(pathElements) == 1 {
		for _, element := range m.Elements {
			if file, ok := element.(*ManifestElementFile); ok {
				if file.Name == pathElements[0] {
					return file
				}
				//fmt.Println(file.Name)
			}
		}
		//fmt.Println("Failed to find : " + pathElements[0])
		return nil
	}

	for _, element := range m.Elements {
		//fmt.Println(element)
		if dir, ok := element.(*ManifestElementDirectory); ok && dir.Name == pathElements[0] {
			return dir.GetFileAtPath(strings.Join(pathElements[1:], "\\"))
		}
	}

	return nil
}

func (m *ManifestElementDirectory) GetDirectoryAtPath(path string) *ManifestElementDirectory {
	pathElements := CleanEmptyElements(strings.Split(path, "\\"))
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
	m.Size = int64(temp["size"].(float64))

	if temp["elements"] == nil {
		return nil
	}
	for _, directory := range temp["elements"].([]interface{}) {
		castedDirectory := directory.(map[string]interface{})
		var directoryData []byte
		directoryData, err = json.Marshal(castedDirectory)
		if err != nil {
			return err
		}
		if ManifestType(int(castedDirectory["type"].(float64))) == MF_Directory {
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
	m.Type = int(temp["type"].(int32))
	m.Checksum = temp["hash"].(string)
	m.Size = temp["size"].(int64)

	if temp["elements"] == nil {
		return nil
	}

	for _, directory := range temp["elements"].(primitive.A) {
		castedDirectory := directory.(map[string]interface{})
		var directoryData []byte
		directoryData, err = bson.Marshal(castedDirectory)
		if err != nil {
			return err
		}
		if ManifestType(int(castedDirectory["type"].(int32))) == MF_Directory {
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

type FileSplitOutput struct {
	LeftFileChecksum  string
	RightFileChecksum string
	HalfSize          int64
	LeftFileSize      int64
	RightFileSize     int64
	Err               error
}

func SplitFile(file *os.File, chunkTargetPath string) *chan FileSplitOutput {
	output := make(chan FileSplitOutput)
	go func() {
		var leftFile, rightFile *os.File
		splitData := FileSplitOutput{}
		sourceStat, err := file.Stat()
		if err != nil {
			fmt.Println("Failed to stat file at line 645: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		if sourceStat.Mode().IsRegular() == false {
			fmt.Println("Not a regular file at line 651")
			splitData.Err = errors.New("not a regular file")
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
			splitData.Err = err
			output <- splitData
			return
		}
		splitData.LeftFileSize, err = io.CopyN(leftFile, file, halfSize)
		if err != nil {
			fmt.Println("Failed to copy right file at line 673: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		_ = leftFile.Close()

		rightFile, err = os.Create(rightFileName)
		if err != nil {
			fmt.Println("Failed to create right file at line 682: ", err)
			splitData.Err = err
			output <- splitData
			return
		}

		splitData.RightFileSize, err = file.Seek(halfSize, 0)
		if err != nil {
			fmt.Println("Failed to seek right file at line 690: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		_, err = io.Copy(rightFile, file)
		if err != nil {
			fmt.Println("Failed to copy right file at line 697: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		_ = rightFile.Close()

		leftFile, err = os.Open(leftFileName)
		if err != nil {
			fmt.Println("Failed to open left file at line 706: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		hash := md5.New()
		if _, err = io.Copy(hash, leftFile); err != nil {
			fmt.Println("Failed to copy left file at line 713: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		splitData.LeftFileChecksum = hex.EncodeToString(hash.Sum(nil))
		hash.Reset()

		rightFile, err = os.Open(rightFileName)
		if err != nil {
			fmt.Println("Failed to open right file at line 724: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		if _, err = io.Copy(hash, rightFile); err != nil {
			fmt.Println("Failed to copy right file at line 730: ", err)
			splitData.Err = err
			output <- splitData
			return
		}

		splitData.RightFileChecksum = hex.EncodeToString(hash.Sum(nil))

		err = rightFile.Close()
		if err != nil {
			fmt.Println("Failed to close right file at line 741: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		err = leftFile.Close()
		if err != nil {
			fmt.Println("Failed to close left file at line 748: ", err)
			splitData.Err = err
			output <- splitData
			return
		}
		err = os.Rename(leftFileName, chunkTargetPath+"\\"+splitData.LeftFileChecksum)
		if err != nil {
			fmt.Println("Failed to rename left file at line 755: ", err)
			fmt.Println(leftFileName)
			fmt.Println(chunkTargetPath + "\\" + splitData.RightFileChecksum)
			splitData.Err = err
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
	fmt.Println("Parsing file : " + filePath)

	go func() {
		manifestOutput := parseFileOutput{}
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Error Parsing File : " + filePath)
			fmt.Println("Failed to open file: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		var fileInfo fs.FileInfo
		fileInfo, err = file.Stat()
		if err != nil {
			fmt.Println("Error Parsing File : " + filePath)
			fmt.Println("Failed to stat file: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		manifestOutput.element.Name = fileInfo.Name()
		manifestOutput.element.Type = int(MF_File)
		manifestOutput.element.Size = fileInfo.Size()

		hash := md5.New()
		if _, err = io.Copy(hash, file); err != nil {
			fmt.Println("Error Parsing File : " + filePath)
			fmt.Println("Failed to copy file: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}

		manifestOutput.checksumHash = hash.Sum(nil)
		manifestOutput.element.Checksum = hex.EncodeToString(manifestOutput.checksumHash)
		if priorManifest != nil {
			priorFile := priorManifest.GetFileAtPath(currentPath + "\\" + manifestOutput.element.Name)
			if priorFile != nil {
				if priorFile.Checksum == manifestOutput.element.Checksum {
					manifestOutput.element.Chunks = priorFile.Chunks
					output <- manifestOutput
					return
				} else {
					fmt.Println("Old Checksum : " + priorFile.Checksum + " New Checksum : " + manifestOutput.element.Checksum)
				}
			}
		}

		priorManifest.DetectedChanges = true

		defer file.Close()
		if fileInfo.Size() <= int64(ChunkSize) {
			var newFile *os.File
			newFile, err = os.Create(chunkTargetPath + "\\" + manifestOutput.element.Checksum)
			if err != nil {
				fmt.Println("Error Parsing File : " + filePath)
				fmt.Println("Failed to create file: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			defer newFile.Close()

			_, _ = file.Seek(0, 0)
			_, err = io.Copy(newFile, file)
			if err != nil {
				fmt.Println("Error Parsing File : " + filePath)
				fmt.Println("Failed to copy file: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			output <- manifestOutput
			return
		}

		_, _ = file.Seek(0, 0)
		numChunks := int(math.Ceil(float64(fileInfo.Size()) / float64(ChunkSize)))
		manifestOutput.element.ChunkSize = int64(ChunkSize)

		for i := 0; i < numChunks; i++ {
			var chunkBytes []byte
			if i == numChunks-1 {
				chunkBytes = make([]byte, fileInfo.Size()%int64(ChunkSize))
			} else {
				chunkBytes = make([]byte, ChunkSize)
			}
			_, _ = file.Read(chunkBytes)

			chunkHash := md5.New()
			_, _ = chunkHash.Write(chunkBytes)
			chunkChecksum := hex.EncodeToString(chunkHash.Sum(nil))
			chunkData := ManifestElementChunk{
				Type:     int(MF_Chunk),
				Checksum: chunkChecksum,
				Size:     int64(len(chunkBytes)),
			}

			manifestOutput.element.Chunks = append(manifestOutput.element.Chunks, chunkData)

			if priorManifest != nil {
				priorChunk := priorManifest.GetChunkAtPath(currentPath + "\\" + manifestOutput.element.Name + "\\" + chunkChecksum)
				if priorChunk != nil && priorChunk.Checksum == chunkChecksum {
					continue
				}
			}

			var chunkFile *os.File
			chunkFile, err = os.Open(chunkTargetPath + "\\" + chunkChecksum)
			if err == nil {
				chunkFile.Close()
				continue
			}

			chunkFile, err = os.Create(chunkTargetPath + "\\" + chunkChecksum)
			if errors.Is(err, os.ErrExist) {
				fmt.Println("Error Parsing File : " + filePath)
				fmt.Println("Failed to create chunk file: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			_, _ = chunkFile.Write(chunkBytes)
			chunkFile.Close()
		}

		fmt.Println("Parsed file : " + filePath)

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
		fmt.Println("Parsing directory : " + filePath)
		manifestOutput := parseDirectoryOutput{}
		manifestOutput.manifestElement.Name = currentPath
		directory, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Error Parsing Directory : " + filePath)
			fmt.Println("Failed to open directory: ", err)
			manifestOutput.err = err
			output <- manifestOutput
			return
		}
		var stat fs.FileInfo
		stat, err = directory.Stat()
		if err != nil {
			fmt.Println("Error Parsing Directory : " + filePath)
			fmt.Println("Failed to stat directory: ", err)
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
		totalSize := int64(0)

		for _, directoryChannel := range directoryChannels {
			directoryOutput := <-*directoryChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &directoryOutput.manifestElement)
			totalSize += directoryOutput.manifestElement.Size

			_, err = fmt.Fprintf(directoryHash, "%x %s\n", directoryOutput.checksumHash, directoryOutput.manifestElement.Name)
			if err != nil {
				fmt.Println("Error Parsing Directory : " + filePath)
				fmt.Println("Failed to write directory hash: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			if directoryOutput.err != nil {
				fmt.Println("Error Parsing Directory : " + filePath)
				fmt.Println("Failed to parse directory: ", directoryOutput.err)
				manifestOutput.err = directoryOutput.err
				output <- manifestOutput
				return
			}
		}

		for _, fileChannel := range fileChannels {
			fileOutput := <-*fileChannel
			manifestOutput.manifestElement.Elements = append(manifestOutput.manifestElement.Elements, &fileOutput.element)
			totalSize += fileOutput.element.Size

			_, err = fmt.Fprintf(directoryHash, "%x %s\n", fileOutput.checksumHash, fileOutput.element.Name)
			if err != nil {
				fmt.Println("Error Parsing Directory : " + filePath)
				fmt.Println("Failed to write file hash: ", err)
				manifestOutput.err = err
				output <- manifestOutput
				return
			}

			if fileOutput.err != nil {
				fmt.Println("Error Parsing Directory : " + filePath)
				fmt.Println("Failed to parse file: ", fileOutput.err)
				manifestOutput.err = fileOutput.err
				output <- manifestOutput
				return
			}
		}

		manifestOutput.manifestElement.Size = totalSize
		manifestOutput.checksumHash = directoryHash.Sum(nil)
		manifestOutput.manifestElement.Checksum = hex.EncodeToString(manifestOutput.checksumHash)

		fmt.Println("Parsed directory : " + filePath)
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
	totalSize := int64(0)

	for _, directoryChannel := range directoryChannels {
		if directoryChannel != nil {
			directoryOutput := <-*directoryChannel
			manifest.Directories = append(manifest.Directories, directoryOutput.manifestElement)
			totalSize += directoryOutput.manifestElement.Size
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
			totalSize += fileOutput.element.Size
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
	manifest.Size = totalSize

	return &manifest, nil
}
