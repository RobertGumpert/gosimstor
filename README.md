# gosimstor

The simplest implementation of a key-value store. The library stores an open file descriptor and map as an index. The card key is the key of the recorded information, and the value is the line number in the file. Reading from a file is done by moving the cursor within the file. The cursor shift parameter is the line number multiplied by the maximum line length. To write data, use the ToString () functions implemented by the user.

# Example

### Create Storage

The store creation function takes as parameters an array of fileProvider objects, which you can create using the NewFileProvider function.
NewFileProvider function parameters:
- fileStorageName, directory string - file and directory name
- maxLengthIncrement int64 - the value by which the value of the constant MAXLENGTH = 1000 is multiplied to get the maximum length of the string
- convertID, convertData ToString - links to functions converting data types to a string.
- convertIdFromString, convertDataFromString FromStringToType - links to functions for converting strings to data types.

```go
dir, err := os.Getwd()
storage, err := NewStorage(
		NewFileProvider(
			"file_1",
			dir,
			3,
			ToStringString,
			ToStringFloat64Vector,
			FromStringToString,
			FromStringToFloat64Vector,
		),
		NewFileProvider(
			"file_2",
			dir,
			5,
			ToStringString,
			ToStringFloat64Vector,
			FromStringToString,
			FromStringToFloat64Vector,
		),
	)
```

##### Example 'ToString' function:

Function convert array of float64 to string

```go
func ToStringFloat64Vector(data interface{}) (string,  error) {
	var (
		convert    string
		elements   = make([]string, 0)
		vector, ok = data.([]float64)
	)
	if !ok {
		return convert, errors.New("DOESN'T CONVERT 'FLOAT64 VECTOR' TO STRING")
	}
	for i := 0; i < len(vector); i++ {
		elements = append(
			elements,
			fmt.Sprintf("%f", vector[i]),
		)
	}
	convert = strings.Join(elements, ",")
	return convert,  nil
}
```

##### Example 'FromStringToType' function:

Function convert string to array of float64

```go
func FromStringToFloat64Vector(data string) (interface{}, error) {
	var (
		split  = strings.Split(data, ",")
		vector = make([]float64, 0)
	)
	for i := 0; i < len(split); i++ {
		element, err := strconv.ParseFloat(split[i], 64)
		if err != nil {
			return nil, err
		}
		vector = append(vector, element)
	}
	return vector, nil
}
```

### Insert

Example Insert data:

```go
inc := strconv.Itoa(i)
key := "Key" + inc
data := []float64{
	1.2 * float64(i), 3.4 * float64(i),
}
err := storage.Insert(
	"file_1",
	key,
	data, 
)
```

### Read

Example Read data:

```go
inc := strconv.Itoa(i)
key := "Key" + inc
id, data, err := storage.Read(
	"file_1",
	key, 
)
```

### Update

Example Update data:

```go
inc := strconv.Itoa(i)
key := "Key" + inc
err := storage.Update(
	"file_1",
	key,
	[]float64{
		1.2 * float64(-1), 3.4 * float64(-1),
	}, 
)
```