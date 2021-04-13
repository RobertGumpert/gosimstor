package gosimstor

import (
	"bufio"
	"errors"
	concurrentMap "github.com/streamrail/concurrent-map"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAXLENGTH = 1000
)

type FileProviderConstructor func() (provider *fileProvider, e error)

type fileProvider struct {
	filePath, fileStorageName, directory string
	file                                 *os.File
	mx                                   *sync.Mutex
	pointers                             concurrentMap.ConcurrentMap
	lastPointer                          int64
	maxLengthLine                        int64
	//
	convertData, convertID                     ToString
	convertDataFromString, convertIdFromString FromStringToType
}

func NewFileProvider(fileStorageName, directory string, maxLengthIncrement int64, convertID, convertData ToString, convertIdFromString, convertDataFromString FromStringToType) FileProviderConstructor {
	return func() (*fileProvider, error) {
		provider := new(fileProvider)
		provider.mx = new(sync.Mutex)
		provider.pointers = concurrentMap.New()
		provider.directory = directory
		provider.fileStorageName = fileStorageName
		provider.convertID = convertID
		provider.convertData = convertData
		provider.convertDataFromString = convertDataFromString
		provider.convertIdFromString = convertIdFromString
		provider.lastPointer = int64(0)
		provider.maxLengthLine = maxLengthIncrement * MAXLENGTH
		if err := provider.initProvider(); err != nil {
			return nil, err
		}
		return provider, nil
	}
}

func (provider *fileProvider) initProvider() error {
	var (
		exist              = false
		fileName, filePath string
		files              []os.FileInfo
		file               *os.File
		err                error
	)
	files, err = ioutil.ReadDir(provider.directory)
	if err != nil {
		return err
	}
	for _, fileInfo := range files {
		if strings.Contains(fileInfo.Name(), provider.fileStorageName) {
			fileName = fileInfo.Name()
			filePath = strings.Join([]string{provider.directory, fileName}, "/")
			exist = true
			break
		}
	}
	if !exist {
		fileName = provider.fileStorageName
		filePath = strings.Join(
			[]string{
				provider.directory,
				strings.Join(
					[]string{
						fileName,
						"simstor",
					},
					".",
				),
			},
			"/",
		)
		file, err = os.Create(filePath)
		if err != nil {
			return err
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
	}
	provider.filePath = filePath
	provider.file = file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		id := strings.Split(line, "=")[0]
		if !provider.pointers.Has(id) {
			provider.pointers.Set(id, provider.lastPointer)
		}
		provider.lastPointer++
	}
	return nil
}

func (provider *fileProvider) Insert(row Row) error {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	return provider.insert(row)
}

func (provider *fileProvider) insert(row Row) error {
	var (
		pointer = provider.lastPointer
		line    string
	)
	stringID, err := provider.convertID(row.ID)
	if err != nil {
		return err
	}
	if exist := provider.pointers.Has(stringID); exist {
		return errors.New("KEY IS EXIST. ")
	}
	stringData, err := provider.convertData(row.Data)
	if err != nil {
		return err
	}
	provider.pointers.Set(stringID, pointer)
	line, err = provider.createLine(stringID, stringData)
	if err != nil {
		return err
	}
	_, err = provider.file.WriteString(line)
	if err != nil {
		provider.pointers.Remove(stringID)
		return err
	}
	provider.lastPointer++
	return err
}

func (provider *fileProvider) Read(id interface{}) (Row, error) {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	return provider.read(id)
}

func (provider *fileProvider) read(id interface{}) (Row, error) {
	var (
		row                    Row
		convertID, convertData interface{}
		stringID, stringData   string
	)
	stringID, stringData, _, err := provider.readLine(id)
	if err != nil {
		return row, err
	}
	convertData, err = provider.convertDataFromString(stringData)
	if err != nil {
		return row, err
	}
	convertID, err = provider.convertIdFromString(stringID)
	if err != nil {
		return row, err
	}
	row.ID = convertID
	row.Data = convertData
	return row, nil
}

func (provider *fileProvider) Update(row Row) error {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	return provider.update(row)
}

func (provider *fileProvider) update(row Row) error {
	var (
		pointer                    int64
		stringID, stringData, line string
	)
	stringID, _, pointer, err := provider.readLine(row.ID)
	if err != nil {
		return err
	}
	stringData, err = provider.convertData(row.Data)
	if err != nil {
		return err
	}
	provider.pointers.Set(stringID, pointer)
	line, err = provider.createLine(stringID, stringData)
	if err != nil {
		return err
	}
	_, err = provider.file.WriteAt([]byte(line), pointer*provider.maxLengthLine)
	return err
}

func (provider *fileProvider) UpdateAll(rows []Row) error {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	err := provider.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = provider.file.Seek(0, 0)
	if err != nil {
		return err
	}
	provider.pointers = concurrentMap.New()
	for i := 0; i < len(rows); i++ {
		err := provider.insert(rows[i])
		if err != nil {
			log.Println("ERR: ", err, "; ROW: ", rows[i])
		}
	}
	return nil
}

func (provider *fileProvider) Rewrite(rows []Row) error {
	provider.mx.Lock()
	var (
		errRewritingFiles error
		newDataBuffer     = concurrentMap.New()
		tempFilePath      string
		tempFile          *os.File
	)
	defer func() {
		if errRewritingFiles != nil {
			var (
				file *os.File
				err  error
			)
			file, err = os.OpenFile(provider.filePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
			if err != nil {
				file, err = os.Create(provider.filePath)
				if err != nil {
					log.Println(err)
					return
				}
			}
			provider.file = file
			if err := provider.deleteTempFile(tempFilePath, tempFile); err != nil {
				log.Println(err)
			}
		}
		provider.mx.Unlock()
		return
	}()
	tempFilePath, tempFile, err := provider.createTempFile()
	if err != nil {
		return err
	}
	for i := 0; i < len(rows); i++ {
		var (
			stringID, stringData, line string
			pointer                    int64
		)
		stringID, err := provider.convertID(rows[i].ID)
		if err != nil {
			if e := provider.deleteTempFile(tempFilePath, tempFile); e != nil {
				err = e
			}
			return err
		}
		stringData, err = provider.convertData(rows[i].Data)
		if err != nil {
			if e := provider.deleteTempFile(tempFilePath, tempFile); e != nil {
				err = e
			}
			return err
		}
		if inter, exist := provider.pointers.Get(stringID); !exist {
			newDataBuffer.Set(stringID, provider.lastPointer)
			pointer = provider.lastPointer + 1
		} else {
			pointer = inter.(int64)
		}
		line, err = provider.createLine(stringID, stringData)
		if err != nil {
			if e := provider.deleteTempFile(tempFilePath, tempFile); e != nil {
				err = e
			}
			return err
		}
		if iter, exist := newDataBuffer.Get(stringID); exist {
			pointer = iter.(int64) * provider.maxLengthLine
		} else {
			pointer = pointer * provider.maxLengthLine
		}
		_, err = tempFile.WriteAt([]byte(line), pointer)
		if err != nil {
			if e := provider.deleteTempFile(tempFilePath, tempFile); e != nil {
				err = e
			}
			return err
		}
	}
	errRewritingFiles = provider.rewriteFile(tempFilePath, tempFile)
	if errRewritingFiles != nil {
		return errRewritingFiles
	}
	for item := range newDataBuffer.IterBuffered() {
		provider.pointers.Set(item.Key, item.Val.(int64))
	}
	provider.mx.Unlock()
	return nil
}

func (provider *fileProvider) GetIDs() []string {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	return provider.pointers.Keys()
}

func (provider *fileProvider) ReadAll() ([]Row, error) {
	provider.mx.Lock()
	defer provider.mx.Unlock()
	var (
		rows = make([]Row, 0)
	)
	scanner := bufio.NewScanner(provider.file)
	for scanner.Scan() {
		line := scanner.Text()
		var (
			row                    Row
			err                    error
			convertID, convertData interface{}
			stringID, stringData   string
		)
		line = strings.Split(line, "|")[0]
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, "=") {
			split := strings.Split(line, "=")
			stringID = split[0]
			if strings.TrimSpace(stringID) == "" {
				continue
			}
			stringData = split[1]
			if strings.TrimSpace(stringData) == "" {
				continue
			}
		} else {
			continue
		}
		convertData, err = provider.convertDataFromString(stringData)
		if err != nil {
			continue
		}
		convertID, err = provider.convertIdFromString(stringID)
		if err != nil {
			continue
		}
		row.ID = convertID
		row.Data = convertData
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (provider *fileProvider) rewriteFile(tempFilePath string, tempFile *os.File) error {
	err := provider.file.Close()
	if err != nil {
		return err
	}
	err = os.Remove(provider.filePath)
	if err != nil {
		return err
	} else {
		provider.file = tempFile
		provider.filePath = tempFilePath
	}
	return nil
}

func (provider *fileProvider) createTempFile() (string, *os.File, error) {
	var (
		tempFile     *os.File
		tempFilePath = strings.ReplaceAll(
			provider.filePath,
			strings.Join([]string{
				provider.fileStorageName,
				"simstor",
			}, "."),
			strings.Join([]string{
				provider.fileStorageName,
				"-rewrite-at-",
				strconv.FormatInt(time.Now().Unix(), 16),
				".simstor",
			}, ""),
		)
	)
	tempFile, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return tempFilePath, tempFile, err
	}
	return tempFilePath, tempFile, nil
}

func (provider *fileProvider) deleteTempFile(tempFilePath string, tempFile *os.File) error {
	err := tempFile.Close()
	if err != nil {
		return err
	}
	err = os.Remove(tempFilePath)
	if err != nil {
		return err
	}
	return nil
}

func (provider *fileProvider) getLine(id interface{}) (string, int64, error) {
	var (
		stringID, err = provider.convertID(id)
		inter         interface{}
		exist         bool
		buffer        = make([]byte, provider.maxLengthLine-1)
		pointer       int64
		line          string
	)
	if err != nil {
		return line, -1, err
	}
	inter, exist = provider.pointers.Get(stringID)
	if !exist {
		return line, -1, errors.New("ELEMENT NOT EXIST BY KEY. ")
	}
	pointer = inter.(int64)
	_, err = provider.file.Seek(pointer*provider.maxLengthLine, 0)
	if err != nil {
		return line, -1, err
	}
	_, err = provider.file.Read(buffer)
	if err != nil {
		return line, -1, err
	}
	line = string(buffer)
	line = strings.Split(line, "|")[0]
	if strings.TrimSpace(line) == "" {
		return line, -1, errors.New("LINE IS EMPTY. ")
	}
	return line, pointer, nil
}

func (provider *fileProvider) readLine(id interface{}) (string, string, int64, error) {
	var (
		line, stringID, stringData string
		pointer                    int64
	)
	line, pointer, err := provider.getLine(id)
	if err != nil {
		return stringID, stringData, pointer, err
	}
	if strings.Contains(line, "=") {
		split := strings.Split(line, "=")
		stringID = split[0]
		if strings.TrimSpace(stringID) == "" {
			return stringID, stringData, pointer, errors.New("STRING ID IS EMPTY. ")
		}
		stringData = split[1]
		if strings.TrimSpace(stringData) == "" {
			return stringID, stringData, pointer, errors.New("STRING DATA IS EMPTY. ")
		}
	} else {
		return stringID, stringData, pointer, errors.New("LINE NOT CONTAINS SEPARATOR. ")
	}
	return stringID, stringData, pointer, nil
}

func (provider *fileProvider) createLine(stringID, toStringData string) (string, error) {
	var (
		line, tail string
		tailBuffer = make([]string, 0)
		tailSize   int64
		sep        = "."
	)
	line = strings.Join([]string{stringID, toStringData}, "=")
	tailSize = provider.maxLengthLine - int64(len(line)) - 2
	if tailSize < 0 {
		return line, errors.New("EXCEEDED PERMISSIBLE LINE LENGTH. ")
	}
	for i := int64(0); i < tailSize; i++ {
		tailBuffer = append(tailBuffer, sep)
	}
	tailBuffer = append(tailBuffer, "\n")
	tail = strings.Join(tailBuffer, "")
	return strings.Join([]string{line, tail}, "|"), nil
}
