package sql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	_ "github.com/lib/pq"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	schema2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	rel "github.com/streamingfast/substreams-sink-sql/pb/test/relations"
	"github.com/test-go/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDatabase_ProcessEntity(t *testing.T) {
	logger, _ := logging.ApplicationLogger("test", "test")

	// Path to your .proto file
	//protoFile := "test/hm/hm.proto"
	protoFile := "test/relations/relations.proto"
	moduleOutputMessage := "test.relations.Output"

	// Create a new parser
	parser := protoparse.Parser{}
	parser.ImportPaths = []string{"/Users/cbillett/devel/sf/substreams-sink-map-sql/proto"}

	// Parse the .proto file to get descriptors
	fds, err := parser.ParseFiles(protoFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse .proto file: %v", err))
	}

	// fds is a []*desc.FileDescriptor, we take the first one for simplicity
	fileDescriptor := fds[0]

	// Print the name of the file
	fmt.Printf("Parsed FileDescriptor: %s\n", fileDescriptor.GetName())

	var rootMessageDescriptor *desc.MessageDescriptor
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == moduleOutputMessage {
			rootMessageDescriptor = messageDescriptor
			break
		}
	}

	schema, err := schema2.NewSchema("rel_test", rootMessageDescriptor, logger)
	require.NoError(t, err)

	db, err := sql.Open("postgres", "dbname=postgres sslmode=disable")
	require.NoError(t, err)

	database, err := NewDatabase(schema, db, "test.relations.Output", rootMessageDescriptor, logger)
	require.NoError(t, err)

	blankCursor, err := sink.NewCursor("")
	if err != nil {
		panic(fmt.Errorf("failed to create cursor: %w", err))
	}

	output := &rel.Output{
		Entities: []*rel.Entity{
			{
				Entity: &rel.Entity_Customer{
					Customer: &rel.Customer{
						CustomerId: "customer.1",
						Name:       "customer.name.1",
					},
				},
			},
			{
				Entity: &rel.Entity_Item{
					Item: &rel.Item{
						ItemId: "item.1",
						Name:   "item.name.1",
						Price:  10.99,
					},
				},
			},
			{
				Entity: &rel.Entity_Item{
					Item: &rel.Item{
						ItemId: "item.2",
						Name:   "item.name.2",
						Price:  19.99,
					},
				},
			},
			{
				Entity: &rel.Entity_Order{
					Order: &rel.Order{
						OrderId:       "order.1",
						CustomerRefId: "customer.1",
						Items: []*rel.OrderItem{
							{
								ItemId:   "item.1",
								Quantity: 10,
							},
							{
								ItemId:   "item.2",
								Quantity: 20,
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(output)
	err = database.ProcessEntity(data, 1, "block.hash.1", time.Now(), blankCursor)
	if err != nil {
		panic(fmt.Errorf("failed to process entity: %w", err))
	}

	return
}
