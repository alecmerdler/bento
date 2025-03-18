package spicedb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func spiceDBOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary("Writes a batch of relationships to SpiceDB.").
		Description("TODO(alecmerdler): Write more detailed description.").
		Fields(
			service.NewURLField("endpoint").
				Description("The URL of the SpiceDB API endpoint.").
				Example("localhost:50051"),
			service.NewStringField("token").
				Description(`SpiceDB API token.`).
				Example("my_token"),
			service.NewTLSField("tls"),
			service.NewStringField("write_schema").
				Description(`The SpiceDB schema to write on start.`).
				Optional().
				Example(exampleWriteSchema),
			service.NewBatchPolicyField("batching"),
			service.NewIntField("max_in_flight").
				Description("The maximum number of requests to have in flight at a given time.").
				Default(64),
			service.NewBloblangField("args_mapping").
				Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to a [Relationship](https://buf.build/authzed/api/docs/main:authzed.api.v1#authzed.api.v1.Relationship).").
				Example(exampleArgsMapping),
		)
}

const exampleWriteSchema = `
definition user {}

definition document {
	relation owner: user

	permission view = owner
}
`

const exampleArgsMapping = `
root = {
	"resource": {
		"object_type": this.resource.type,
		"object_id": this.resource.id
	},
	"relation": this.relation,
	"subject": {
		"object": {
			"object_type": "user",
			"object_id": this.user.id
		}
	}
}
`

func init() {
	err := service.RegisterBatchOutput(
		"spicedb", spiceDBOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			out, err = newSpiceDBWriter(conf, mgr)
			return
		},
	)
	if err != nil {
		panic(err)
	}
}

type spiceDBWriter struct {
	endpoint *url.URL
	token    string
	client   *authzed.Client
	connMut  sync.Mutex

	write_schema string
	argsMapping  *bloblang.Executor

	res    *service.Resources
	logger *service.Logger
}

func newSpiceDBWriter(conf *service.ParsedConfig, res *service.Resources) (*spiceDBWriter, error) {
	var err error
	w := &spiceDBWriter{
		res:    res,
		logger: res.Logger(),
	}

	w.endpoint, err = conf.FieldURL("endpoint")
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint field: %w", err)
	}

	w.token, err = conf.FieldString("token")
	if err != nil {
		return nil, fmt.Errorf("failed to parse token field: %w", err)
	}

	if conf.Contains("write_schema") {
		if w.write_schema, err = conf.FieldString("write_schema"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("args_mapping") {
		if w.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (w *spiceDBWriter) Connect(ctx context.Context) (err error) {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	// TODO(alecmerdler): Add TLS fields to the config and pass them when creating client...
	transportCredentialsOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	tokenOption := grpcutil.WithInsecureBearerToken(w.token)
	w.client, err = authzed.NewClient(w.endpoint.String(), tokenOption, transportCredentialsOption)
	if err != nil {
		err = fmt.Errorf("failed to create client: %w", err)
	}

	if w.write_schema != "" {
		if _, err = w.client.WriteSchema(ctx, &v1.WriteSchemaRequest{
			Schema: w.write_schema,
		}); err != nil {
			err = fmt.Errorf("failed to write schema: %w", err)
		}
	}

	return
}

func (w *spiceDBWriter) WriteBatch(context context.Context, batch service.MessageBatch) error {
	var executor *service.MessageBatchBloblangExecutor
	if w.argsMapping != nil {
		executor = batch.BloblangExecutor(w.argsMapping)
	}

	updates := make([]*v1.RelationshipUpdate, 0, len(batch))
	for i := range batch {
		msg, err := executor.Query(i)
		if err != nil {
			return err
		}

		raw, err := msg.AsBytes()
		if err != nil {
			return err
		}

		var rel v1.Relationship
		if err := json.Unmarshal(raw, &rel); err != nil {
			return err
		}

		if err := rel.ValidateAll(); err != nil {
			return err
		}

		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: &rel,
		})
	}

	resp, err := w.client.WriteRelationships(context, &v1.WriteRelationshipsRequest{
		Updates: updates,
	})
	if err != nil {
		return err
	}

	w.logger.With("batch_size", len(updates), "zed_token", resp.WrittenAt.Token).Debug("wrote relationships batch to SpiceDB.")

	return nil
}

func (w *spiceDBWriter) Close(ctx context.Context) (err error) {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	return
}
