/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spez

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	// This field is read-only.
	ID string

	// Data is the actual data in the message.
	Data []byte

	// Attributes represents the key-value pairs the current message
	// is labelled with.
	Attributes map[string]string

	// The time at which the message was published.
	// This is populated by the server for Messages obtained from a subscription.
	// This field is read-only.
	PublishTime time.Time
	// contains filtered or unexported fields
}

// Consumes a Pub/Sub message and writes it to an GCS Object
func archiver(ctx context.Context, m PubSubMessage) error {

	// Creates a client.
	client, err := storage.NewClient(ctx)

	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Sets the name for the new bucket.
	// this is the spanner table name
	bucketName := m.Attributes["Topic"]

	// Creates a Bucket instance.
	bucket := client.Bucket(bucketName)

	// Creates random 4 letters for record UUID
	//rand.Seed(time.Now().UnixNano())
	id := make([]rune, 4)
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for x := range id {
		id[x] = letters[rand.Intn(len(letters))]
	}

	appendix := string(id)
	timestamp := time.Now().String()

	// Create fileName
	var sb strings.Builder
	sb.WriteString(timestamp)
	sb.WriteString("-")
	sb.WriteString(appendix)
	fileName := sb.String()

	wc := bucket.Object(fileName).NewWriter(ctx)
	wc.ContentType = "avro/bytes"
	wc.Metadata = map[string]string{
		"x-spez-message-id":   m.ID,
		"x-spez-publish-time": m.PublishTime.String(),
		"x-spez-timestamp":    m.Attributes["Timestamp"],
	}

	if _, err := wc.Write(m.Data); err != nil {
		log.Printf("createFile: unable to write data to bucket %q, file %q: %v", bucketName, fileName, err)
		return nil
	}

	if err := wc.Close(); err != nil {
		log.Printf("createFile: unable to close bucket %q, file %q: %v", bucketName, fileName, err)
		return nil
	}

	return nil
}
