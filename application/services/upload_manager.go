package services

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
)

type VideoUpload struct {
	Paths        []string
	VideoPath    string
	OutputBucket string
	Errors       []string
}

func NewVideoUpload() *VideoUpload {
	return &VideoUpload{}
}

func (vu *VideoUpload) UploadObject(objectPath string, client *storage.Client, ctx context.Context) error {

	path := strings.Split(objectPath, os.Getenv("localStoragePath")+"/")

	f, err := os.Open(objectPath)
	if err != nil {
		return err
	}

	wc := client.Bucket(vu.OutputBucket).Object(path[1]).NewWriter(ctx)
	//TODO: verificar doc ACL
	//2025/02/13 18:05:09 error during the upload: /tmp/76e6443f-a1ba-4e72-84b5-a6df851f2da2/video/avc1/seg-11.m4s. Error: googleapi: Error 400: Cannot insert legacy ACL for an object when uniform bucket-level access is enabled. Read more at https://cloud.google.com/storage/docs/uniform-bucket-level-access, invalid
	//wc.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}

	if _, err = io.Copy(wc, f); err != nil {
		return err
	}

	if err := wc.Close(); err != nil {
		return err
	}

	defer f.Close()

	return nil
}

func (vu *VideoUpload) ProcessUpload(concurrency int, doneUpload chan string) error {

	in := make(chan int, runtime.NumCPU()) // qual o arquivo baseado na posicao do slice Paths
	returnChannel := make(chan string)

	err := vu.loadPaths()
	if err != nil {
		return err
	}

	uploadClient, ctx, err := getClientUpload()
	if err != nil {
		return err
	}

	for process := 0; process < concurrency; process++ {
		go vu.uploadWorker(in, returnChannel, uploadClient, ctx)
	}

	go func() {
		for x := 0; x < len(vu.Paths); x++ {
			in <- x
		}
	}()

	countDoneWorker := 0
	for r := range returnChannel {
		countDoneWorker++

		if r != "" {
			doneUpload <- r
			break
		}

		if countDoneWorker == len(vu.Paths) {
			close(in)
		}
	}

	return nil
}

func (vu *VideoUpload) uploadWorker(in chan int, returnChan chan string, uploadClient *storage.Client, ctx context.Context) {

	for x := range in {
		err := vu.UploadObject(vu.Paths[x], uploadClient, ctx)

		if err != nil {
			vu.Errors = append(vu.Errors, vu.Paths[x])
			log.Printf("error during the upload: %v. Error: %v", vu.Paths[x], err)
			returnChan <- err.Error()
		}

		returnChan <- ""
	}

	returnChan <- "upload completed"
}

func (vu *VideoUpload) loadPaths() error {

	err := filepath.Walk(vu.VideoPath, func(path string, info os.FileInfo, err error) error {

		if !info.IsDir() {
			vu.Paths = append(vu.Paths, path)
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func getClientUpload() (*storage.Client, context.Context, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	return client, ctx, nil
}
