package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/msgregistry"
	flag "github.com/ogier/pflag"
	"github.com/pkg/profile"
	gi "github.com/sabhiram/go-gitignore"
)

const PROTO_ROOT string = "PBCAT_PROTO_ROOT"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var expression = flag.String("match", "", "Match only records. Format: FieldName='my value'")
var count = flag.BoolP("count", "c", false, "Print number of matches")
var maxMatches = flag.Int("m", 0, "Maximum number of matches to output")
var protoRoot = flag.StringP("proto-root", "p", os.Getenv(PROTO_ROOT), "Path to the root of the protos to be added")
var protoMessage = flag.String("msg", "", "Fully qualified name of the proto")
var skipParseIgnore = flag.Bool("skip-ignore", false, "Skip .gitignore files")
var useJsonpb = flag.Bool("jsonpb", false, "Whether to use jsonpb instead of encoding/json, (more correct results but 7x slower for structs)")

func listExtension(files *[]string, extension string, ignore *gi.GitIgnore) filepath.WalkFunc {
	// Add . to extension if missing
	if !strings.HasPrefix(extension, ".") {
		extension = "." + extension
	}

	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}

		if ignore != nil {
			if info.IsDir() && ignore.MatchesPath(path) {
				return filepath.SkipDir
			}

			if ignore.MatchesPath(path) {
				return nil
			}
		}

		// Exclude directories
		if info.IsDir() {
			return nil
		}

		// Filter on files with the given extension
		if filepath.Ext(path) == extension {
			// Keep the path relative to the root
			path = strings.TrimPrefix(path, *protoRoot)
			*files = append(*files, path)
		}
		return nil
	}
}

func createMessageRegistry(protoRoot string) (*msgregistry.MessageRegistry, []string) {
	msgreg := msgregistry.NewMessageRegistryWithDefaults()

	msgUrls := make([]string, 0)

	p := protoparse.Parser{ImportPaths: []string{protoRoot}}

	var ignore *gi.GitIgnore
	if !*skipParseIgnore {
		var err error
		ignore, err = gi.CompileIgnoreFile(path.Join(protoRoot, ".gitignore"))
		if err != nil {
			log.Println(err)
			log.Println("Proceeding without .gitignore")
		}
	}

	var files []string
	err := filepath.Walk(protoRoot, listExtension(&files, "proto", ignore))
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		fds, err := p.ParseFiles(file)
		if err != nil {
			log.Fatal(err)
		}
		for _, fd := range fds {
			msgreg.AddFile("", fd)
			for _, msgType := range fd.GetMessageTypes() {
				msgUrls = append(msgUrls, msgType.GetFullyQualifiedName())
			}
		}
	}

	return msgreg, msgUrls
}

type MatchExpr struct {
	Field  string
	Regexp *regexp.Regexp
}

func main() {
	flag.Parse()

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: pbcat [options] file\n\n")
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var file *os.File
	l := flag.NArg()
	if l > 1 {
		flag.Usage()
		os.Exit(2)
	} else if l == 1 {
		fileName := flag.Arg(0)
		var err error
		file, err = os.Open(fileName)
		if err != nil {
			log.Fatalf("Error opening input file '%v'", fileName)
		}
	} else {
		file = os.Stdin
	}

	if *cpuprofile != "" {
		// Enable CPU profiling
		defer profile.Start(profile.ProfilePath(*cpuprofile)).Stop()
	}
	if *memprofile != "" {
		// Enable memory profiling
		defer profile.Start(profile.MemProfile, profile.ProfilePath(*memprofile), profile.NoShutdownHook).Stop()
	}

	nline := []byte("\n")

	if *protoRoot == "" {
		log.Fatalf("No root directory set for protos.\n"+
			"Please make sure that the environment variable %s is set\n"+
			"or use --proto-files/-p flag to pass a root directory.",
			PROTO_ROOT)
	}

	var matchExpr *MatchExpr
	if *expression != "" {
		s := strings.Split(*expression, "=")
		matchExpr = &MatchExpr{s[0], regexp.MustCompile(s[1])}
	}

	readConcurrency := 30
	serializeConcurrency := 16
	registry, registryUrls := createMessageRegistry(*protoRoot)

	var msgURL string
	if *protoMessage != "" {
		msgURL = *protoMessage
	} else {
		if len(registryUrls) == 0 {
			log.Fatalf("No .proto files found under the root: \"%s\"\n"+
				"Please make sure that the environment variable %s is set\n"+
				"or use --proto-root/-p flag to pass a root directory.",
				*protoRoot, PROTO_ROOT)
		}
		msgURL = inferMessageType(*file, registry, registryUrls)
		if msgURL == "" {
			log.Fatalf("Message type could not be infered")
		}
		log.Printf("Infered type: %s\n", msgURL)
	}

	out := bufio.NewWriterSize(os.Stdout, 16384)
	defer out.Flush()

	msgLocations := readMessageLocations(*file, *maxMatches)
	if *count && matchExpr == nil {
		// Exit early, if we only want to count and no match regexp
		c := 0
		for range msgLocations {
			c++
		}
		fmt.Printf("%d\n", c)
		return
	}

	msgs := readMessages(*file, msgURL, registry, readConcurrency, msgLocations)
	matched := match(msgs, matchExpr)

	// Conly count matches, don't print jsonl
	if *count {
		c := 0
		for range matched {
			c++
		}
		out.WriteString(strconv.Itoa(c))
		return
	}

	serialized := jsonpSerialize(matched, serializeConcurrency, *useJsonpb)

	for b := range serialized {
		out.Write(b)
		out.Write(nline)
		if matchExpr != nil {
			// If we're matching records flush everytime.
			out.Flush()
		}
	}
}

func jsonpSerialize(msgs chan proto.Message, concurrency int, useJsonpb bool) chan []byte {
	out := make(chan []byte)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			marsh := &jsonpb.Marshaler{OrigName: true}
			for msg := range msgs {
				if useJsonpb {
					var buf bytes.Buffer
					marsh.Marshal(&buf, msg)
					out <- buf.Bytes()
				} else {
					res, _ := json.Marshal(msg)
					out <- res
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func match(msgs <-chan proto.Message, matchExpr *MatchExpr) chan proto.Message {
	numMatches := 0
	out := make(chan proto.Message)
	go func() {
		defer close(out)
		for msg := range msgs {
			if *maxMatches > 0 && numMatches >= *maxMatches {
				// We're done here.
				return
			}

			if matchExpr != nil {
				dm, _ := dynamic.AsDynamicMessage(msg)
				v, err := dm.TryGetFieldByName(matchExpr.Field)
				if err != nil {
					fmt.Println("Cant find fied '" + matchExpr.Field + "'")
					continue
				}
				if matchExpr.Regexp.MatchString(v.(string)) {
					// Line matches the expression, write it
					out <- msg
					numMatches++
				}
			} else {
				// Write it
				out <- msg
				numMatches++
			}
		}
	}()
	return out
}

type messageLocation struct {
	loc     int
	msgSize uint64
}

func readMessageLocations(file os.File, maxMessages int) <-chan messageLocation {
	reader := bufio.NewReaderSize(&file, 64*1024)

	out := make(chan messageLocation)

	go func() {
		defer close(out)
		numMessages := 0
		location := 0
		for {
			if maxMessages > 0 && numMessages >= maxMessages {
				// We're done here.
				return
			}

			varIntBuf, err := reader.Peek(10)
			if err != nil {
				// Probably EOF
				return
			}

			nb, numRead := proto.DecodeVarint(varIntBuf)

			if numRead == 0 {
				log.Fatal(numRead)
			}
			location += numRead
			out <- messageLocation{location, nb}
			location += int(nb)
			reader.Discard(numRead + int(nb))

			numMessages++
		}
	}()

	return out
}

func matchMessageType(file os.File, msgLocations <-chan messageLocation, msgreg *msgregistry.MessageRegistry, types []string) []string {
	matches := types
	for msgLocation := range msgLocations {

		msgBuf := make([]byte, msgLocation.msgSize)
		if _, err := file.ReadAt(msgBuf, int64(msgLocation.loc)); err != nil {
			log.Fatal(err)
		}
		// Reset the offset for future reads
		file.Seek(0, 0)

		matches = make([]string, 0)
		for _, url := range types {
			msg, err := msgreg.Resolve(url)
			if err != nil {
				log.Fatal(err)
			}

			// Try to Unmarshal the buffer to this msg
			if err := proto.Unmarshal(msgBuf, msg); err != nil {
				continue
			}

			// Check that the protos are the same size, discard unknown fields
			dm, ok := msg.(*dynamic.Message)
			if ok {
				if len(dm.GetUnknownFields()) != 0 {
					continue
				}
			} else {
				proto.DiscardUnknown(msg)
			}

			if proto.Size(msg) == int(msgLocation.msgSize) {
				matches = append(matches, url)
			}
		}
		types = matches
	}
	return matches
}

func inferMessageType(file os.File, msgreg *msgregistry.MessageRegistry, registryUrls []string) string {
	numRecordsToTry := 10
	msgLocations := readMessageLocations(file, numRecordsToTry)

	matchedTypes := matchMessageType(file, msgLocations, msgreg, registryUrls)

	if len(matchedTypes) == 0 {
		return ""
	}

	if len(matchedTypes) == 1 {
		return matchedTypes[0]
	}

	log.Println("Multiple matches:", strings.Join(matchedTypes, ", "))

	log.Println("Attempting to match by name...")
	matchesOnName := make([]string, 0)
	for _, match := range matchedTypes {
		s := strings.Split(match, ".")
		protoName := s[1]
		if strings.Contains(strings.ToLower(file.Name()), strings.ToLower(protoName)) {
			matchesOnName = append(matchesOnName, match)
		}
	}

	if len(matchesOnName) == 1 {
		return matchesOnName[0]
	}

	return ""
}

func readMessages(file os.File, msgname string, msgreg *msgregistry.MessageRegistry, concurrency int, msgLocations <-chan messageLocation) <-chan proto.Message {
	var wg sync.WaitGroup
	wg.Add(concurrency)

	out := make(chan proto.Message)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			msgBuf := make([]byte, 256*1024)
			for msgLocation := range msgLocations {

				if _, err := file.ReadAt(msgBuf[:msgLocation.msgSize], int64(msgLocation.loc)); err != nil {
					// Probably EOF
					return
				}

				msg, err := msgreg.Resolve(msgname)
				if err != nil {
					log.Fatal(err)
				}

				if err := proto.Unmarshal(msgBuf[:msgLocation.msgSize], msg); err != nil {
					log.Fatal(err)
				}
				out <- msg
			}

		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
